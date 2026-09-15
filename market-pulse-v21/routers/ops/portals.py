"""The four portal homes and the staff API.

Phase 1 is the skeleton: enough of each surface to prove a session
reaches it, that the scoping layer is between it and the data, and that
the wrong portal is refused. The features hang off this in Phases 2-15.

Every route declares its scope. `deps.current_scope` reads that
declaration off the endpoint and refuses a route that has none, so
adding a handler here without a `@scoped` line fails the suite before
merge and returns 500 rather than data if one ever slips through.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from lib.ops import repository as RP
from lib.ops import routeguard as RG
from lib.ops import scope as S

from . import deps

log = logging.getLogger("mf.web.portals")
router = APIRouter()
templates = Jinja2Templates(directory="templates")

PORTAL_LABEL = {
    "staff": "Staff", "owner": "Owner", "tenant": "Resident",
    "vendor": "Vendor",
}


def _home(portal: str) -> None:
    """One home page per portal, with the portal closed over — never a
    parameter, for the reason spelled out in auth_routes.py."""
    label = PORTAL_LABEL[portal]

    @router.get(f"/ops/{portal}/", response_class=HTMLResponse,
                name=f"ops_{portal}_home")
    @RG.scoped("mf_users", "read", portals={portal},
               note=f"{portal} portal home")
    async def home(request: Request, scope: S.Scope = Depends(deps.current_scope),
                   repo: RP.Repository = Depends(deps.repository)):
        me = repo.fetch_one("mf_users", scope.user_id,
                            columns=["id", "email", "full_name", "title"])
        return templates.TemplateResponse("ops/home.html", {
            "request": request, "portal": portal, "label": label,
            "me": me, "roles": sorted(scope.roles),
            "divisions": list(scope.divisions),
            # Rendered so the operator can see what the session actually
            # is. A portal that does not show which role you are holding
            # is a portal where the wrong one goes unnoticed.
            "visible_people": repo.count("mf_users"),
        })


for _p in sorted(S.PORTALS):
    _home(_p)
del _p


# ── the staff API ──
# JSON, staff portal only. This is the route the acceptance criterion
# points at: a tenant session hitting it directly must get 403, not a
# redirect to a login page it is already past.
@router.get("/ops/api/users", name="ops_api_users")
@RG.scoped("mf_users", "read", portals={"staff"},
           note="staff directory; scoped to the caller's divisions")
async def api_users(repo: RP.Repository = Depends(deps.repository),
                    limit: int = 100):
    rows = repo.fetch("mf_users", columns=["id", "email", "full_name",
                                           "title", "division_id",
                                           "is_active"],
                      order="id ASC", limit=limit)
    return JSONResponse({"users": rows, "count": len(rows)})


@router.get("/ops/api/divisions", name="ops_api_divisions")
@RG.scoped("mf_divisions", "read", portals={"staff"},
           note="division list for the staff console")
async def api_divisions(repo: RP.Repository = Depends(deps.repository)):
    rows = repo.fetch("mf_divisions",
                      columns=["id", "name", "description", "archived_at"],
                      order="name ASC")
    return JSONResponse({"divisions": rows, "count": len(rows)})


@router.get("/ops/api/audit", name="ops_api_audit")
@RG.scoped("mf_audit_log", "read", portals={"staff"},
           note="platform_admin only — mf_audit_log has no division column, "
                "so scope.py denies every narrower role")
async def api_audit(repo: RP.Repository = Depends(deps.repository),
                    limit: int = 100):
    rows = repo.fetch("mf_audit_log",
                      columns=["id", "occurred_at", "actor_user_id", "action",
                               "target_type", "target_id"],
                      order="id DESC", limit=limit)
    return JSONResponse({"entries": [
        {**r, "occurred_at": r["occurred_at"].isoformat()} for r in rows],
        "count": len(rows)})


# ── administration ──
@router.post("/ops/api/divisions", name="ops_api_division_create")
@RG.scoped("mf_divisions", "create", portals={"staff"},
           note="platform_admin only, per the grants matrix")
async def api_create_division(name: str = Form(...),
                              description: str = Form(""),
                              repo: RP.Repository = Depends(deps.repository)):
    new_id = repo.insert("mf_divisions", {"name": name,
                                          "description": description})
    if new_id is None:
        raise HTTPException(403, "not authorised")
    return JSONResponse({"id": new_id}, status_code=201)


@router.post("/ops/api/users", name="ops_api_user_create")
@RG.scoped("mf_users", "create", portals={"staff"},
           note="platform_admin or a division manager, within their division")
async def api_create_user(email: str = Form(...), full_name: str = Form(""),
                          division_id: int = Form(None),
                          repo: RP.Repository = Depends(deps.repository)):
    try:
        new_id = repo.insert("mf_users", {
            "email": email, "full_name": full_name,
            "division_id": division_id})
    except RP.RepositoryError as e:
        # A cross-division create. 403 rather than 400: the request was
        # well-formed, the caller simply may not make it.
        raise HTTPException(403, str(e))
    if new_id is None:
        raise HTTPException(403, "not authorised")
    return JSONResponse({"id": new_id}, status_code=201)


@router.post("/ops/api/users/{user_id}/roles", name="ops_api_grant_role")
@RG.scoped("mf_user_roles", "create", portals={"staff"},
           note="rank must strictly dominate; enforced in scope.may_grant "
                "and again by the mf_user_roles_scope_ck trigger")
async def api_grant_role(user_id: int, role: str = Form(...),
                         division_id: int = Form(None),
                         repo: RP.Repository = Depends(deps.repository)):
    if role not in S.ROLES:
        raise HTTPException(400, "unknown role")
    new_id = repo.grant_role(user_id, role, division_id=division_id)
    if new_id is None:
        raise HTTPException(403, "not authorised to grant that role")
    return JSONResponse({"id": new_id}, status_code=201)

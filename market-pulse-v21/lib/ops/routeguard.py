"""Every /ops route declares what it needs, or the test suite fails.

ARCHITECTURE.md §2, guard 3: "a route with no declared scope fails the
suite rather than defaulting to open, which is how `_admin_gate` fails
today". That last clause is the whole argument. On the analysis side,
adding a route makes it public until somebody remembers to gate it, and
forgetting produces no error at any point — the page just works, for
everyone. Here, forgetting produces a failing test before merge.

The declaration is data attached to the endpoint function:

    @router.get("/ops/staff/users")
    @scoped("mf_users", "read", portals={"staff"})
    async def list_users(...): ...

and `undeclared()` walks a router's routes and returns the ones missing
it. There is no default. A route that genuinely needs no scope says so
with `@public()`, which is a declaration too — the point is that
somebody decided, and the decision is visible in the diff.

`enforce()` is the runtime half: given a scope and a declaration it
returns the reason to refuse, or None. Phase 1-D wires it in as a
FastAPI dependency; it is a plain function so the authorization matrix
can be tested without an HTTP server.
"""
from __future__ import annotations

from dataclasses import dataclass

from lib.ops import scope as S

ATTR = "__ops_scope__"


@dataclass(frozen=True)
class Declaration:
    entity: str | None
    action: str = "read"
    portals: frozenset = frozenset()
    public: bool = False
    note: str = ""

    def __post_init__(self):
        if self.public:
            return
        if self.entity not in S.ENTITIES:
            raise ValueError(
                f"{self.entity!r} is not a known ops entity. A route cannot "
                f"declare a scope over a table the scoping layer has never "
                f"heard of — that is exactly the combination that would be "
                f"unenforceable at runtime.")
        if self.action not in S.ACTIONS:
            raise ValueError(f"{self.action!r} is not a known action.")
        bad = set(self.portals) - S.PORTALS
        if bad:
            raise ValueError(f"unknown portal(s): {sorted(bad)}")
        if not self.portals:
            raise ValueError(
                "a route must name the portal(s) it serves. An unrestricted "
                "route is reachable from a tenant session, and the whole "
                "reason portals exist is that one person can hold both a "
                "tenant role and a staff role.")


def scoped(entity: str, action: str = "read", portals=(), note: str = ""):
    """Declare what a route needs. Attaches data; changes no behaviour."""
    decl = Declaration(entity=entity, action=action,
                       portals=frozenset(portals), note=note)

    def deco(fn):
        setattr(fn, ATTR, decl)
        return fn
    return deco


def public(note: str):
    """Declare that a route needs no scope. A note is REQUIRED.

    Making the escape hatch cost a sentence of justification is the
    entire design: it is still available for a login page or a health
    check, and it is impossible to use without leaving a reason behind
    for whoever reads the diff.
    """
    if not note or not note.strip():
        raise ValueError(
            "public() needs a reason. An unauthenticated ops route with no "
            "explanation is indistinguishable from one somebody forgot to "
            "protect.")
    decl = Declaration(entity=None, public=True, note=note)

    def deco(fn):
        setattr(fn, ATTR, decl)
        return fn
    return deco


def declaration(endpoint) -> Declaration | None:
    return getattr(endpoint, ATTR, None)


def undeclared(router, prefix: str = "/ops") -> list[str]:
    """Routes under `prefix` with no declaration. Empty is the passing state.

    Takes anything with a `.routes` list — a FastAPI app, an APIRouter,
    or a stand-in — so the guard can be tested against a router built for
    the purpose rather than only against whatever happens to be mounted.
    """
    missing = []
    for route in getattr(router, "routes", []):
        path = getattr(route, "path", "")
        if not path.startswith(prefix):
            continue
        endpoint = getattr(route, "endpoint", None)
        if endpoint is None:
            continue
        if declaration(endpoint) is None:
            methods = ",".join(sorted(getattr(route, "methods", []) or ["*"]))
            missing.append(f"{methods} {path}")
    return sorted(missing)


def enforce(scope: S.Scope | None, decl: Declaration | None) -> str | None:
    """The reason to refuse, or None to allow.

    Returns a string rather than raising so the caller decides the status
    code and the log line, and so the matrix can be asserted directly.
    """
    if decl is None:
        # Belt and braces with the test. If an undeclared route somehow
        # reaches runtime, it is closed, not open.
        return ("this route declares no scope; refusing rather than "
                "guessing what it should require")
    if decl.public:
        return None
    if scope is None:
        return "not authenticated"
    if scope.portal not in decl.portals:
        # The acceptance criterion: a tenant session hitting a staff
        # route is refused here, before any query is built, whatever
        # roles the underlying user may also hold.
        return (f"this route serves the {'/'.join(sorted(decl.portals))} "
                f"portal; this session is on the {scope.portal} portal")
    pred = S.visible(scope, decl.entity, decl.action)
    if pred.denied:
        return pred.reason
    return None

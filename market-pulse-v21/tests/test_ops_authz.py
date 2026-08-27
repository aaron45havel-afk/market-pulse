"""The role × entity × action matrix, and the ways it is supposed to say no.

Run:  python tests/test_ops_authz.py      (exit 0 = all pass)

Pure — no database. Every decision in lib/ops/scope.py is a decision
about strings and integers, which is why it is a separate module from
the repository: the whole authorization matrix can be asserted in
milliseconds, and there is no excuse for not asserting all of it.

The checks are weighted towards refusals. A permission test that only
proves the allowed cases work is a test that passes against a system
which allows everything, and that system passes a demo.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib.ops import routeguard as RG
from lib.ops import scope as S

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


ORG = 1


def sc(portal, *grants, user_id=10, org=ORG):
    return S.Scope(user_id=user_id, organization_id=org, portal=portal,
                   grants=tuple(grants))


G = S.RoleGrant
ADMIN = sc("staff", G("platform_admin"))
DM_A = sc("staff", G("division_manager", division_id=1), user_id=20)
DM_B = sc("staff", G("division_manager", division_id=2), user_id=21)
STAFF = sc("staff", G("staff", division_id=1), user_id=30)
TENANT = sc("tenant", G("tenant", division_id=1), user_id=40)
OWNER = sc("owner", G("owner_client", division_id=1), user_id=50)
VENDOR = sc("vendor", G("vendor", division_id=1), user_id=60)


# ── the default is DENY ──
check(S.visible(ADMIN, "mf_leases").denied,
      "AN UNKNOWN ENTITY IS DENIED EVEN TO A PLATFORM ADMIN. Phase 2 adds "
      "mf_leases; until somebody writes its access rule it is invisible, "
      "rather than inheriting whatever the broadest role happens to have")
check(S.visible(ADMIN, "pm_holdings").denied,
      "and an analysis-side table is not reachable through ops scoping at "
      "all — the seam runs both ways")
check("no entry in scope.ENTITIES" in S.visible(ADMIN, "mf_leases").reason,
      "and the refusal says why, so the fix is obvious to whoever hits it")

check(S.visible(sc("staff"), "mf_users").denied,
      "a scope with NO GRANTS sees nothing, rather than defaulting to the "
      "lowest role")
check(raises(S.ScopeError, S.Scope, user_id=1, organization_id=1,
             portal="admin"),
      "an unknown portal is refused at construction — a scope with a "
      "portal nobody recognises cannot be separated from any other")
check(raises(S.ScopeError, S.RoleGrant, "superuser"),
      "and an unknown role cannot be granted")
check(raises(S.ScopeError, ADMIN.can, "mf_users", "delete"),
      "'delete' is not an action. Ops rows are archived — a deleted lease "
      "is a deleted defence in a dispute")


# ── portal separation ──
# One person, two roles. This is the case CLAUDE.md's four portals exist
# for, and the one that a role-only model gets wrong.
BOTH = S.Scope(user_id=70, organization_id=ORG, portal="tenant",
               grants=(G("staff", division_id=1), G("tenant", division_id=1)))
check(BOTH.roles == {"tenant"},
      "A USER WHO IS BOTH STAFF AND A TENANT, LOGGED IN THROUGH THE TENANT "
      "PORTAL, IS A TENANT. Their staff grant is not in play — the union "
      "of everything they hold is never what a single session gets")
BOTH_STAFF = S.Scope(user_id=70, organization_id=ORG, portal="staff",
                     grants=BOTH.grants)
check(BOTH_STAFF.roles == {"staff"},
      "and through the staff portal the same person is staff, not both")
check(S.visible(BOTH, "mf_users").sql.endswith("id = %s")
      and 70 in S.visible(BOTH, "mf_users").params,
      "on the tenant portal they can read exactly one user row: their own")
check(not S.visible(BOTH_STAFF, "mf_users").denied
      and "division_id = ANY(%s)" in S.visible(BOTH_STAFF, "mf_users").sql,
      "on the staff portal the same person reads their division")

check(sc("tenant", G("platform_admin")).roles == frozenset(),
      "a platform admin who opens a TENANT session holds no admin rights "
      "in it — the portal filters the grant, it does not merely label it")
check(S.visible(sc("tenant", G("platform_admin")), "mf_users").denied,
      "so that session reads nothing at all, which is correct: it holds no "
      "tenant grant either")


# ── organization scoping ──
p = S.visible(ADMIN, "mf_users")
check("organization_id = %s" in p.sql and ORG in p.params,
      "even a platform admin is scoped to their organization — 'admin sees "
      "everything' stops being safe the day there are two operators in one "
      "database")
check(not S.visible(ADMIN, "mf_users").denied, "and does see their own org")


# ── division scoping ──
pa = S.visible(DM_A, "mf_users")
pb = S.visible(DM_B, "mf_users")
check("division_id = ANY(%s)" in pa.sql and [1] in pa.params,
      "a division manager's reads are narrowed to their division")
check([2] in pb.params and [1] not in pb.params,
      "AND THE TWO MANAGERS GET DIFFERENT PREDICATES. If they did not, the "
      "filter would be decoration")
check(S.visible(DM_A, "mf_audit_log").denied,
      "a division manager cannot read the audit log, because mf_audit_log "
      "has no division column and there is no honest way to scope them to "
      "part of it — 'scoped to nothing' quietly means all of it")
check(not S.visible(ADMIN, "mf_audit_log").denied,
      "the platform admin can, being org-wide by definition")

DM_NOWHERE = sc("staff", G("division_manager"), user_id=22)
check(S.visible(DM_NOWHERE, "mf_users").denied,
      "A DIVISION MANAGER WITH NO DIVISION SEES NOTHING. The tempting bug "
      "is to read a missing scope as 'unrestricted'; that turns a "
      "misconfigured row into an org-wide grant")
check(not DM_NOWHERE.org_wide,
      "and org_wide is false for them — only platform_admin can be "
      "deliberately unscoped")
check(ADMIN.org_wide, "which the platform admin is")


# ── self-only roles ──
for s, who in ((TENANT, "tenant"), (OWNER, "owner"), (VENDOR, "vendor")):
    p = S.visible(s, "mf_users")
    check(not p.denied and f"id = %s" in p.sql and s.user_id in p.params,
          f"a {who} reads exactly one user row — their own")
    check("division_id = ANY(%s)" not in p.sql,
          f"a {who} does NOT get their division's rows even though the "
          f"grant carries a division_id — the division on an outside "
          f"role scopes what they belong to, not what they may read")
    check(S.visible(s, "mf_user_roles").denied,
          f"a {who} cannot read role assignments, their own included — "
          f"who else holds what is not their business")
    check(S.visible(s, "mf_audit_log").denied, f"nor the audit log")
    check(S.visible(s, "mf_divisions").denied, f"nor the division list")

check(S.visible(TENANT, "mf_users", "update").sql.count("%s") == 2,
      "a tenant may update their own contact details, still scoped to "
      "org and self")
check(S.visible(TENANT, "mf_users", "create").denied,
      "but may not create a user")
check(S.visible(TENANT, "mf_users", "archive").denied, "or archive one")

check(S.visible(TENANT, "mf_documents").denied,
      "AND NOBODY BELOW A PLATFORM ADMIN READS DOCUMENTS IN PHASE 1. "
      "mf_documents has a visibility column that looks like the filter — "
      "but visibility='tenant' would show every tenant every "
      "tenant-visible document in the organization, neighbours' leases "
      "included. What actually scopes a document is its lease, and "
      "mf_leases is Phase 2")
check(S.visible(OWNER, "mf_documents").denied
      and S.visible(VENDOR, "mf_documents").denied,
      "same for owners and vendors")
check(S.visible(STAFF, "mf_documents").denied
      and S.visible(DM_A, "mf_documents").denied,
      "AND FOR STAFF AND DIVISION MANAGERS TOO — for a different reason "
      "that lands in the same place: mf_documents has organization_id but "
      "no division_id, so a division-scoped role cannot be narrowed to "
      "its own division's documents. Widening to the organization is the "
      "silent escalation this layer exists to prevent")
check(not S.visible(ADMIN, "mf_documents").denied,
      "the platform admin can, being org-wide by definition")


# ── reference data ──
for s in (ADMIN, DM_A, STAFF, TENANT, OWNER, VENDOR):
    p = S.visible(s, "mf_jurisdictions")
    check(not p.denied and p.sql == "",
          f"every authenticated role reads jurisdictions unfiltered — "
          f"California is a fact about the world, not about this operator "
          f"({s.portal})")
check(S.visible(TENANT, "mf_jurisdiction_rules", "update").denied
      and S.visible(DM_A, "mf_jurisdiction_rules", "update").denied,
      "but nobody below platform_admin edits a rule — the rules table IS "
      "the legal knowledge, and a wrong row there is wrong everywhere")
check(not S.visible(ADMIN, "mf_jurisdiction_rules", "update").denied,
      "the platform admin can")


# ── staff cannot do management things ──
check(S.visible(STAFF, "mf_users", "create").denied,
      "staff cannot create users")
check(S.visible(STAFF, "mf_user_roles").denied,
      "or see role assignments at all")
check(S.visible(STAFF, "mf_organizations").denied,
      "or the organization row")
check(S.visible(STAFF, "mf_sessions").denied,
      "or the session table — knowing who is logged in is a management "
      "capability")
check(not S.visible(STAFF, "mf_users").denied,
      "but staff do read their division's people, which is the job")


# ── privilege escalation ──
check(S.may_grant(ADMIN, "platform_admin"),
      "a platform admin may grant at their own level — somebody has to be "
      "able to appoint a successor")
check(S.may_grant(DM_A, "staff") and S.may_grant(DM_A, "tenant"),
      "a division manager may grant the roles below them")
check(not S.may_grant(DM_A, "division_manager"),
      "BUT NOT ANOTHER DIVISION MANAGER. Rank must strictly dominate, or "
      "'I'll clone my access for a colleague' makes rank meaningless "
      "within a week")
check(not S.may_grant(DM_A, "platform_admin"),
      "and certainly not a platform admin — this is the escalation path "
      "that matters")
check(not S.may_grant(STAFF, "tenant"),
      "STAFF GRANT NOTHING, EVEN A ROLE THEY OUT-RANK. Rank alone would "
      "say yes — 20 < 60 — but staff hold no create permission on "
      "mf_user_roles at all. A predicate named may_grant that returns "
      "True for someone who may not grant is a trap for the next caller "
      "who trusts the name")
check(not S.may_grant(STAFF, "staff"), "nor their own level")
check(not S.may_grant(TENANT, "tenant"), "and a tenant grants nothing")
check(raises(S.ScopeError, S.may_grant, ADMIN, "root"),
      "an unknown role name is refused rather than compared")


# ── columns that never leave ──
check(raises(S.ScopeError, S.selectable, "mf_users", ["id", "password_hash"]),
      "NOBODY SELECTS A PASSWORD HASH, platform admin included. 'The admin "
      "can see everything' is how a stolen admin session becomes a stolen "
      "authenticator")
check(raises(S.ScopeError, S.selectable, "mf_users", ["mfa_secret"]),
      "nor a TOTP seed, which would let the holder mint valid codes "
      "forever without ever touching the account")
check(raises(S.ScopeError, S.selectable, "mf_sessions", ["token_hash"]),
      "nor a session token hash")
check(S.selectable("mf_users", ["id", "email"]) == ["id", "email"],
      "ordinary columns pass through unchanged")


# ── the route guard ──
d = RG.Declaration("mf_users", "read", frozenset({"staff"}))
check(RG.enforce(TENANT, d) is not None,
      "A TENANT SESSION IS REFUSED ON A STAFF ROUTE. This is Phase 1's "
      "acceptance criterion, decided before a query is built and "
      "regardless of what roles the underlying user also holds")
check("portal" in RG.enforce(TENANT, d),
      "and the refusal names the portal mismatch rather than the scope, "
      "because that is the actual reason")
check(RG.enforce(BOTH, d) is not None,
      "including for the person who genuinely IS staff, when they are on "
      "the tenant portal — which is the case a role check alone passes")
check(RG.enforce(BOTH_STAFF, d) is None,
      "and the same person on the staff portal is allowed")
check(RG.enforce(STAFF, d) is None, "as is ordinary staff")
check(RG.enforce(None, d) is not None, "an unauthenticated caller is refused")
check(RG.enforce(TENANT, None) is not None,
      "AND A ROUTE WITH NO DECLARATION IS CLOSED, NOT OPEN. The suite is "
      "supposed to catch that before merge; this is what happens if one "
      "ever reaches runtime")

check(RG.enforce(STAFF, RG.Declaration("mf_users", "create",
                                       frozenset({"staff"}))) is not None,
      "the declaration's ACTION is enforced too — staff pass the portal "
      "check on a create route and are still refused")

check(raises(ValueError, RG.Declaration, "mf_leases", "read",
             frozenset({"staff"})),
      "a route cannot declare a scope over an entity the scoping layer "
      "does not know — that combination would be unenforceable at runtime")
check(raises(ValueError, RG.Declaration, "mf_users", "read", frozenset()),
      "or over no portal at all")
check(raises(ValueError, RG.Declaration, "mf_users", "read",
             frozenset({"lobby"})),
      "or a portal that does not exist")
check(raises(ValueError, RG.public, ""),
      "public() without a reason is refused — an unauthenticated ops route "
      "with no explanation is indistinguishable from one somebody forgot")
check(RG.public("login form, by definition pre-authentication") is not None,
      "with a reason it works")


# ── guard 3: the enumerator finds an undeclared route ──
# Built here rather than run only against the real app, because there are
# no /ops routes yet: a guard whose first real exercise is the day it
# matters is a guard nobody has tested.
class _Route:
    def __init__(self, path, endpoint, methods=("GET",)):
        self.path, self.endpoint, self.methods = path, endpoint, set(methods)


class _Router:
    def __init__(self, routes):
        self.routes = routes


@RG.scoped("mf_users", "read", portals={"staff"})
def _declared_route():
    pass


@RG.public("health check, no data")
def _public_route():
    pass


def _forgotten_route():
    pass


def _analysis_route():
    pass


_router = _Router([
    _Route("/ops/staff/users", _declared_route),
    _Route("/ops/health", _public_route),
    _Route("/ops/staff/reports", _forgotten_route, methods=("GET", "POST")),
    _Route("/screener", _analysis_route),
])
missing = RG.undeclared(_router)
check(missing == ["GET,POST /ops/staff/reports"],
      f"THE ENUMERATOR FINDS EXACTLY THE ROUTE NOBODY DECLARED, and "
      f"neither the declared one, the deliberately public one, nor the "
      f"analysis-side route outside /ops (got {missing})")
check(RG.undeclared(_Router([_Route("/ops/x", _declared_route)])) == [],
      "a fully-declared router is clean")
check(RG.declaration(_declared_route).portals == frozenset({"staff"}),
      "the declaration survives on the function for the runtime check to "
      "read")
check(RG.declaration(_forgotten_route) is None, "and is absent when omitted")


# ── the role list matches the database ──
# scope.py hard-codes role keys because authorization has to work without
# a connection. This is the check that keeps the two copies honest; the
# database side is asserted in tests/test_ops_schema.py.
import re
from pathlib import Path

_sql = (Path(__file__).resolve().parent.parent / "lib" / "ops" / "migrations"
        / "0001_foundation.up.sql").read_text()
_seeded = set(re.findall(r"^\s*\('([a-z_]+)',\s*'[^']+',\s*\d+,\s*(?:TRUE|FALSE)\)",
                         _sql, re.M))
check(_seeded == S.ROLES,
      f"THE ROLES IN scope.py ARE EXACTLY THE ROLES THE MIGRATION SEEDS. A "
      f"role in the database with no entry here has no permissions and "
      f"fails silently; one here with no row cannot be granted. "
      f"(migration: {sorted(_seeded)}, scope.py: {sorted(S.ROLES)})")


# ── report ──
if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for m in _FAILS:
        print("  ✗", m)
    sys.exit(1)
print(f"OK — all {_COUNT} ops-authz checks passed.")
print("   Default deny, portal separation, division isolation, no "
      "escalation,\n   and a tenant session refused on a staff route.")
sys.exit(0)

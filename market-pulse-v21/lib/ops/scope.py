"""Who can see which rows — as data, checked in one place, denying by default.

CLAUDE.md asks for authorization in the data layer rather than in UI
routing. ARCHITECTURE.md §2 explains why it is not Postgres row-level
security: this app connects as one database user with a per-call
connection and no session context, so RLS would need a
`SET LOCAL app.current_user` plumbed into every checkout. That is the
better answer eventually and is in BACKLOG.md. This is the answer that
fits what is actually deployed.

The shape:

    Scope           who is asking, through which portal, with which grants
    ENTITIES        what each mf_ table is — its org/division/self columns
    GRANTS          role × entity × action, written out, no wildcards
    visible()       a SQL predicate, or a refusal

Three properties are load-bearing, and each has a test that fails if it
is removed:

  * DEFAULT DENY. An entity nobody wrote a rule for is invisible. Phase 2
    adds properties, units and leases; if someone adds the table and
    forgets the rule, every query against it returns nothing and the
    tests say so. The opposite default — unknown means allowed — is how
    `_admin_gate` on the analysis side behaves today, and it is why a new
    route there is open until someone remembers to close it.

  * PORTAL SEPARATION. One person can be both a staff member and a
    tenant. Which of those they are RIGHT NOW is the portal their
    session was opened through, not the union of everything they hold.
    A tenant-portal session carries no staff grants even for a user who
    has them.

  * NO ESCALATION. A grant cannot exceed the rank of the person making
    it. A division manager cannot create a platform admin, including by
    granting the role to themselves.

Pure: no database, no network, no clock. Everything here is a decision
about strings and integers, so the whole matrix is testable without a
connection.
"""
from __future__ import annotations

from dataclasses import dataclass, field

# ── roles ──
# These keys must match the rows seeded by 0001_foundation. They are
# repeated here rather than read from the database because scope
# decisions have to work without a connection — and
# tests/test_ops_authz.py asserts the two lists are identical, so a
# divergence is a test failure rather than a silent authorization hole.
ROLE_RANK = {
    "platform_admin": 100,
    "division_manager": 80,
    "staff": 60,
    "owner_client": 40,
    "tenant": 20,
    "vendor": 20,
}
ROLES = frozenset(ROLE_RANK)

# ── portals ──
# CLAUDE.md: four surfaces. The portal is recorded on the session, not on
# the user, which is what lets one person hold two roles safely.
PORTALS = frozenset({"staff", "owner", "tenant", "vendor"})
PORTAL_ROLES = {
    "staff": frozenset({"platform_admin", "division_manager", "staff"}),
    "owner": frozenset({"owner_client"}),
    "tenant": frozenset({"tenant"}),
    "vendor": frozenset({"vendor"}),
}

ACTIONS = frozenset({"read", "create", "update", "archive"})
# "delete" is deliberately absent. Ops rows are archived, never deleted:
# a deleted lease is a deleted defence in a dispute. The one table that
# genuinely cannot be deleted from enforces it in Postgres as well
# (mf_audit_log), because a convention is not a guarantee.


class ScopeError(PermissionError):
    pass


# ── what each table is ──
@dataclass(frozen=True)
class Entity:
    """How a table connects to the org / division / person hierarchy.

    A column named here is one `visible()` may filter on. `pii` marks a
    table whose reads must be audited — CLAUDE.md requires an audit
    record for every read of tenant PII, and marking it on the table is
    the only version of that rule nobody can forget to apply.
    """
    table: str
    org_col: str | None = "organization_id"
    division_col: str | None = None
    self_col: str | None = None        # column holding the subject's user id
    pii: bool = False
    reference: bool = False            # shared lookup data, no org scoping


ENTITIES: dict[str, Entity] = {
    "mf_organizations": Entity("mf_organizations", org_col="id"),
    "mf_divisions": Entity("mf_divisions", division_col="id"),
    "mf_users": Entity("mf_users", division_col="division_id",
                       self_col="id", pii=True),
    "mf_user_roles": Entity("mf_user_roles", org_col=None,
                            division_col="division_id", self_col="user_id"),
    "mf_documents": Entity("mf_documents", division_col=None, pii=True),
    "mf_jobs": Entity("mf_jobs", org_col=None),
    "mf_sessions": Entity("mf_sessions", org_col=None, self_col="user_id"),
    "mf_audit_log": Entity("mf_audit_log", org_col=None),
    # Reference data. Jurisdictions and their rules are not org-scoped —
    # California is California for everyone — so they are readable by any
    # authenticated scope and writable by nobody below platform_admin.
    "mf_roles": Entity("mf_roles", org_col=None, reference=True),
    "mf_jurisdictions": Entity("mf_jurisdictions", org_col=None, reference=True),
    "mf_jurisdiction_rules": Entity("mf_jurisdiction_rules", org_col=None,
                                    reference=True),
}

# Columns that leave the database for nobody, at any privilege level.
# A platform admin has no legitimate need to read a password hash or a
# TOTP seed, and "the admin can see everything" is how a stolen admin
# session becomes a stolen authenticator.
NEVER_SELECT = {
    "mf_users": frozenset({"password_hash", "mfa_secret"}),
    "mf_sessions": frozenset({"token_hash"}),
}


# ── the matrix ──
# role -> entity -> actions. Written out per entity ON PURPOSE. A wildcard
# would mean Phase 2's mf_leases is readable by whoever holds the wildcard
# the day it is created, before anyone has thought about it.
def _r(*entities):
    return {e: frozenset({"read"}) for e in entities}


_REFERENCE_READ = _r("mf_roles", "mf_jurisdictions", "mf_jurisdiction_rules")

GRANTS: dict[str, dict[str, frozenset]] = {
    "platform_admin": {
        **_REFERENCE_READ,
        "mf_organizations": frozenset({"read", "update"}),
        "mf_divisions": frozenset({"read", "create", "update", "archive"}),
        "mf_users": frozenset({"read", "create", "update", "archive"}),
        "mf_user_roles": frozenset({"read", "create", "archive"}),
        "mf_documents": frozenset({"read", "create", "update", "archive"}),
        "mf_jobs": frozenset({"read", "create"}),
        "mf_sessions": frozenset({"read", "archive"}),
        "mf_audit_log": frozenset({"read"}),
        "mf_jurisdiction_rules": frozenset({"read", "create", "update"}),
    },
    "division_manager": {
        **_REFERENCE_READ,
        "mf_divisions": frozenset({"read"}),
        "mf_users": frozenset({"read", "create", "update"}),
        "mf_user_roles": frozenset({"read", "create", "archive"}),
        "mf_jobs": frozenset({"read"}),
        "mf_sessions": frozenset({"read"}),
        # NOT mf_documents — see the note on the outside roles below. The
        # table has organization_id but no division_id, so a
        # division-scoped role cannot be narrowed to its own division's
        # documents, and visible() denies rather than widening to the
        # organization. That refusal is correct and this grant would have
        # been dead weight hiding it.
        # NOT mf_audit_log. The log has no division column, so there is no
        # honest way to scope a division manager's read of it — and
        # "scoped to nothing" quietly means "all of it". Phase 2 gives
        # audit rows a division and this opens then. Until it does, the
        # answer is no rather than a filter that does not filter.
    },
    "staff": {
        **_REFERENCE_READ,
        "mf_divisions": frozenset({"read"}),
        "mf_users": frozenset({"read"}),
    },
    # The three outside roles can reach their own user row and the
    # reference tables, and nothing else yet.
    #
    # NOBODY BELOW platform_admin READS mf_documents IN PHASE 1, and the
    # omission is the interesting part. The table has a `visibility`
    # column ('internal'|'owner'|'tenant'|'vendor'), so `visibility =
    # 'tenant'` looks like the obvious filter — and it would show every
    # tenant every tenant-visible document in the organization, including
    # the neighbours' leases. What actually scopes a document is its
    # owner_type/owner_id pointing at a lease or a property, and
    # mf_leases arrives in Phase 2. A filter that filters the wrong thing
    # is worse than no access, so: no access, until there is a join to
    # scope by. The same reasoning removes it from staff and division
    # managers, who have no division column on the table to narrow by.
    "owner_client": {
        **_REFERENCE_READ,
        "mf_users": frozenset({"read"}),       # self only, via visible()
    },
    "tenant": {
        **_REFERENCE_READ,
        "mf_users": frozenset({"read", "update"}),   # own contact details
    },
    "vendor": {
        **_REFERENCE_READ,
        "mf_users": frozenset({"read"}),
    },
}

# Roles whose visibility is limited to their own row, whatever the grant
# says. A tenant with "read mf_users" reads one user: themselves.
SELF_ONLY = frozenset({"tenant", "owner_client", "vendor"})


# ── who is asking ──
@dataclass(frozen=True)
class RoleGrant:
    role: str
    division_id: int | None = None
    property_id: int | None = None

    def __post_init__(self):
        if self.role not in ROLES:
            raise ScopeError(f"Unknown role {self.role!r}.")


@dataclass(frozen=True)
class Scope:
    """One request's authority. Built by the session layer, never by a route."""
    user_id: int
    organization_id: int
    portal: str
    grants: tuple[RoleGrant, ...] = ()
    privilege_epoch: int = 1

    def __post_init__(self):
        if self.portal not in PORTALS:
            raise ScopeError(
                f"Unknown portal {self.portal!r}. A scope with no portal "
                f"cannot be separated from one with a different portal, "
                f"which is the point of having them.")

    # ── portal separation ──
    @property
    def effective(self) -> tuple[RoleGrant, ...]:
        """Grants this PORTAL admits. Not everything the user holds.

        Someone who is both a maintenance supervisor and a tenant in one
        of the buildings has two sets of rights and must never hold both
        at once. Logged in through the tenant portal they are a tenant,
        full stop.
        """
        allowed = PORTAL_ROLES[self.portal]
        return tuple(g for g in self.grants if g.role in allowed)

    @property
    def roles(self) -> frozenset:
        return frozenset(g.role for g in self.effective)

    @property
    def rank(self) -> int:
        return max((ROLE_RANK[g.role] for g in self.effective), default=0)

    @property
    def divisions(self) -> tuple[int, ...]:
        """Divisions this scope reaches, deduplicated and ordered."""
        return tuple(sorted({g.division_id for g in self.effective
                             if g.division_id is not None}))

    @property
    def org_wide(self) -> bool:
        """True only for a grant that is deliberately unscoped.

        0001_foundation's mf_user_roles_scope_ck permits an unscoped
        grant for platform_admin alone, so this cannot be reached by an
        ordinary role no matter what a caller passes.
        """
        return any(g.division_id is None and g.role == "platform_admin"
                   for g in self.effective)

    def can(self, entity: str, action: str) -> bool:
        """Does any effective role permit this action on this entity?"""
        if action not in ACTIONS:
            raise ScopeError(
                f"Unknown action {action!r}. Actions are a closed set so a "
                f"typo is a refusal, not an unchecked path.")
        return any(action in GRANTS.get(g.role, {}).get(entity, ())
                   for g in self.effective)


@dataclass(frozen=True)
class Predicate:
    """A WHERE fragment, its parameters, and whether anything is visible."""
    sql: str = ""
    params: tuple = ()
    allowed: bool = True
    reason: str = ""

    @property
    def denied(self) -> bool:
        return not self.allowed


DENY = Predicate(sql="FALSE", params=(), allowed=False)


def deny(reason: str) -> Predicate:
    return Predicate(sql="FALSE", params=(), allowed=False, reason=reason)


# ── the function everything goes through ──
def visible(scope: Scope, entity: str, action: str = "read") -> Predicate:
    """The rows `scope` may touch in `entity`, as SQL.

    Returns a denying predicate rather than raising, so the repository
    can log the refusal and return an empty result with the same code
    path it uses for "you may look, there is nothing there". A route that
    treats those differently leaks the existence of rows it was not
    allowed to see.
    """
    ent = ENTITIES.get(entity)
    if ent is None:
        # THE DEFAULT. A table with no entry here is invisible.
        return deny(f"{entity} has no entry in scope.ENTITIES — a table "
                    f"nobody has written an access rule for is denied, not "
                    f"opened.")
    if not scope.effective:
        return deny(f"no grant admitted by the {scope.portal} portal")
    if not scope.can(entity, action):
        return deny(f"no effective role permits {action} on {entity}")

    # Reference data is not org-scoped: a jurisdiction is a fact about
    # the world, not about this operator.
    if ent.reference:
        return Predicate()

    clauses, params = [], []

    if ent.org_col:
        clauses.append(f"{ent.org_col} = %s")
        params.append(scope.organization_id)

    # Self-only roles collapse to one row regardless of everything else.
    if scope.roles & SELF_ONLY:
        if not ent.self_col:
            return deny(
                f"{entity} has no column identifying its subject, so a "
                f"self-only role cannot be scoped to their own rows — "
                f"denying rather than showing them everyone's")
        clauses.append(f"{ent.self_col} = %s")
        params.append(scope.user_id)
        return Predicate(" AND ".join(clauses), tuple(params))

    if scope.org_wide:
        return Predicate(" AND ".join(clauses) if clauses else "",
                         tuple(params))

    # Division-scoped. A role granted on a division sees that division's
    # rows; if the table has no division column there is nothing to
    # narrow by, and widening to the whole organization would be exactly
    # the silent escalation this module exists to prevent.
    divs = scope.divisions
    if not divs:
        return deny("a non-admin grant with no division is scoped to "
                    "nothing — an unscoped non-admin role is a "
                    "misconfiguration, not a wildcard")
    if not ent.division_col:
        return deny(
            f"{entity} has no division column, so a division-scoped role "
            f"cannot be narrowed to its division. Widening to the whole "
            f"organization here is how a division manager quietly reads "
            f"another division's records.")
    clauses.append(f"{ent.division_col} = ANY(%s)")
    params.append(list(divs))
    return Predicate(" AND ".join(clauses), tuple(params))


def selectable(entity: str, columns: list[str] | None = None) -> list[str]:
    """Columns a query may ask for, with the never-select set removed.

    Passing None means "everything the caller is allowed to see", which
    the repository expands explicitly rather than emitting SELECT * —
    a star silently starts returning any secret a later migration adds.
    """
    banned = NEVER_SELECT.get(entity, frozenset())
    if columns is None:
        return []          # repository expands from information_schema
    bad = [c for c in columns if c in banned]
    if bad:
        raise ScopeError(
            f"{', '.join(bad)} may not be selected from {entity} by anyone. "
            f"A password hash or TOTP seed leaving the database turns a "
            f"stolen session into a stolen authenticator.")
    return list(columns)


def may_grant(scope: Scope, role: str) -> bool:
    """Can this scope hand out `role`?

    TWO conditions, and the second was nearly left out. Rank must
    strictly dominate — a division manager cannot mint another division
    manager, which stops the "I'll clone my own access for a colleague"
    path that makes rank meaningless within a week. Only a platform
    admin, already at the top, may grant at its own level.

    But rank alone would say a staff member (60) can grant a tenant role
    (20), which is false: staff hold no create permission on
    mf_user_roles at all. The repository would have caught it on the
    insert, so nothing was actually exploitable — and a predicate named
    may_grant that returns True for someone who may not grant is a trap
    for the next caller who trusts the name. Both conditions, here.
    """
    if role not in ROLES:
        raise ScopeError(f"Unknown role {role!r}.")
    if not scope.can("mf_user_roles", "create"):
        return False
    if "platform_admin" in scope.roles:
        return True
    return ROLE_RANK[role] < scope.rank


def require(scope: Scope, entity: str, action: str = "read") -> Predicate:
    """visible(), but raising. For call sites that cannot express a refusal."""
    p = visible(scope, entity, action)
    if p.denied:
        raise ScopeError(f"{action} on {entity} denied: {p.reason}")
    return p

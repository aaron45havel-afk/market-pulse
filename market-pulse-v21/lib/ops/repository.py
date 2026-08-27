"""The only code allowed to build SQL against an mf_ table.

ARCHITECTURE.md §2, guard 2: "no raw conn.execute against an mf_* table
outside it". tests/test_ops_schema.py enforces that by scanning the
source tree, so this is not a style preference — a query written
anywhere else fails the suite.

Everything a route can do to ops data passes through here, and here is
where three things happen that would otherwise happen sometimes:

  1. scope.visible() is consulted and its predicate is ANDed into every
     statement. Not "usually" — there is no code path that builds a
     WHERE without it.
  2. A refusal is logged and returns EMPTY, with the same shape as a
     permitted query that found nothing. A route that can distinguish
     "denied" from "no rows" leaks the existence of records the caller
     was not allowed to know about.
  3. A read of a table marked pii=True writes an audit row before the
     data is returned.

NO FREE-FORM SQL CROSSES THIS BOUNDARY. Callers pass a filter dict, not
a WHERE string. A `where: str` parameter would be convenient and would
be the injection hole in this application — every column name is checked
against information_schema and every value is a bound parameter. The
cost is that complex queries need a method here; that cost is the point.

Transactions: this never commits. The caller owns the transaction so
that a write and the audit row describing it land together or not at
all.
"""
from __future__ import annotations

import logging

from lib.ops import audit as A
from lib.ops import scope as S

log = logging.getLogger("mf.repo")

DEFAULT_LIMIT = 200
MAX_LIMIT = 2000

# Writable only through a dedicated, deliberate call — never through the
# generic insert/update path. Reading them is banned outright by
# scope.NEVER_SELECT; this is the other half.
CREDENTIAL_COLUMNS = {
    "mf_users": frozenset({"password_hash", "mfa_secret"}),
    "mf_sessions": frozenset({"token_hash"}),
}


class RepositoryError(RuntimeError):
    pass


class Repository:
    def __init__(self, conn, scope: S.Scope, request_id: str | None = None):
        self.conn = conn
        self.scope = scope
        self.request_id = request_id
        self._columns: dict[str, list[str]] = {}

    # ── introspection ──
    def columns(self, entity: str) -> list[str]:
        """Real column names, from the database, minus the never-readable.

        Read from information_schema rather than hard-coded so that a
        column added by a later migration is available without editing
        this file — and so that a column REMOVED by one produces an
        error here instead of a query that silently returns nothing.
        """
        if entity in self._columns:
            return self._columns[entity]
        if entity not in S.ENTITIES:
            raise RepositoryError(f"{entity} is not a known ops entity.")
        cur = self.conn.cursor()
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = %s "
            "ORDER BY ordinal_position", (entity,))
        cols = [r[0] for r in cur.fetchall()]
        cur.close()
        if not cols:
            raise RepositoryError(
                f"{entity} has no columns — the table does not exist. Run "
                f"migrations.")
        banned = S.NEVER_SELECT.get(entity, frozenset())
        self._columns[entity] = [c for c in cols if c not in banned]
        return self._columns[entity]

    def _check_columns(self, entity: str, names) -> None:
        known = set(self.columns(entity)) | set(
            CREDENTIAL_COLUMNS.get(entity, ()))
        unknown = [n for n in names if n not in known]
        if unknown:
            raise RepositoryError(
                f"{entity} has no column(s) {', '.join(sorted(unknown))}. "
                f"Column names are checked against the database rather than "
                f"interpolated, which is what keeps this layer free of "
                f"injection.")

    # ── filters ──
    def _where(self, entity: str, filters: dict | None):
        """A filter dict becomes bound SQL. Values are never interpolated.

        Supported: column=value, column=[a, b] (IN), column=None (IS NULL).
        Anything richer needs a method on this class, deliberately — the
        alternative is a query language, and a query language reachable
        from a route is the injection surface again in a new hat.
        """
        if not filters:
            return "", []
        self._check_columns(entity, filters.keys())
        clauses, params = [], []
        for col, val in filters.items():
            if val is None:
                clauses.append(f"{col} IS NULL")
            elif isinstance(val, (list, tuple, set)):
                vals = list(val)
                if not vals:
                    # An empty IN list matches nothing. Say so explicitly
                    # rather than emitting `IN ()`, which is a syntax
                    # error, or dropping the clause, which would silently
                    # widen the query to everything.
                    clauses.append("FALSE")
                else:
                    clauses.append(f"{col} = ANY(%s)")
                    params.append(vals)
            else:
                clauses.append(f"{col} = %s")
                params.append(val)
        return " AND ".join(clauses), params

    def _scoped(self, entity: str, action: str, filters: dict | None):
        """Predicate + filters, or None if scope refuses.

        The refusal is audited here so that no caller can forget to. A
        denied attempt is more interesting than most successful ones.
        """
        pred = S.visible(self.scope, entity, action)
        if pred.denied:
            log.info("denied %s %s for user=%s portal=%s: %s", action, entity,
                     self.scope.user_id, self.scope.portal, pred.reason)
            try:
                A.record_denied(self.conn, self.scope, entity, action,
                                pred.reason, self.request_id)
            except Exception as e:      # pragma: no cover - defensive
                log.error("could not record denial: %s", e)
            return None
        fsql, fparams = self._where(entity, filters)
        parts = [p for p in (pred.sql, fsql) if p]
        return (" AND ".join(parts) or "TRUE",
                list(pred.params) + fparams)

    # ── reads ──
    def fetch(self, entity: str, filters: dict | None = None,
              columns: list[str] | None = None, order: str | None = None,
              limit: int = DEFAULT_LIMIT, offset: int = 0) -> list[dict]:
        scoped = self._scoped(entity, "read", filters)
        if scoped is None:
            return []       # identical shape to "allowed, found nothing"
        where, params = scoped

        cols = self.columns(entity) if columns is None else S.selectable(
            entity, columns)
        self._check_columns(entity, cols)

        sql = f"SELECT {', '.join(cols)} FROM {entity} WHERE {where}"
        if order:
            # Ordering is a column name plus an optional direction, checked
            # the same way everything else is. Accepting an ORDER BY string
            # would hand back the injection surface the filter dict closes.
            col, _, direction = order.partition(" ")
            self._check_columns(entity, [col])
            direction = direction.strip().upper()
            if direction not in ("", "ASC", "DESC"):
                raise RepositoryError(f"Bad sort direction {direction!r}.")
            sql += f" ORDER BY {col} {direction}".rstrip()
        sql += " LIMIT %s OFFSET %s"
        params = params + [min(int(limit), MAX_LIMIT), max(0, int(offset))]

        cur = self.conn.cursor()
        cur.execute(sql, params)
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        cur.close()

        ent = S.ENTITIES[entity]
        if ent.pii and rows:
            A.record_read(self.conn, self.scope, entity,
                          ids=[r.get("id") for r in rows if "id" in r],
                          count=len(rows), request_id=self.request_id)
        return rows

    def fetch_one(self, entity: str, row_id, columns=None) -> dict | None:
        rows = self.fetch(entity, {"id": row_id}, columns=columns, limit=1)
        return rows[0] if rows else None

    def count(self, entity: str, filters: dict | None = None) -> int:
        """How many rows the caller may see. Not how many exist.

        Deliberately not audited: a count discloses no PII, and auditing
        it would bury the reads that matter under pagination noise.
        """
        scoped = self._scoped(entity, "read", filters)
        if scoped is None:
            return 0
        where, params = scoped
        cur = self.conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {entity} WHERE {where}", params)
        n = cur.fetchone()[0]
        cur.close()
        return int(n)

    def exists(self, entity: str, row_id) -> bool:
        return self.count(entity, {"id": row_id}) > 0

    # ── writes ──
    def _reject_floats(self, entity: str, values: dict) -> None:
        """No float reaches an mf_ table, at runtime as well as in the schema.

        tests/test_ops_schema.py greps the migrations so no float COLUMN
        can be created. This is the other direction: a float VALUE headed
        for a BIGINT cents column would be coerced by the driver, and
        1999.9999999998 becoming 1999 is a silent loss that reconciles
        wrong later. lib/ops/money.py refuses floats at parse time for
        the same reason; this catches the paths that skipped it.
        """
        bad = [k for k, v in values.items() if isinstance(v, float)]
        if bad:
            raise RepositoryError(
                f"float value(s) for {', '.join(sorted(bad))} on {entity}. "
                f"Money is integer minor units (lib/ops/money.py); anything "
                f"else that is genuinely fractional needs a considered "
                f"column type, not a float slipped in through a write.")

    def _reject_credentials(self, entity: str, values: dict) -> None:
        bad = set(values) & CREDENTIAL_COLUMNS.get(entity, frozenset())
        if bad:
            raise RepositoryError(
                f"{', '.join(sorted(bad))} cannot be written through the "
                f"generic repository path. Credentials are set by a "
                f"dedicated call that hashes, audits, and bumps "
                f"privilege_epoch — three things a generic update would "
                f"skip in silence.")

    def insert(self, entity: str, values: dict) -> int | None:
        """Create a row inside the caller's scope. Returns its id, or None
        if scope refused."""
        pred = S.visible(self.scope, entity, "create")
        if pred.denied:
            log.info("denied create %s for user=%s: %s", entity,
                     self.scope.user_id, pred.reason)
            A.record_denied(self.conn, self.scope, entity, "create",
                            pred.reason, self.request_id)
            return None

        values = dict(values)
        self._check_columns(entity, values.keys())
        self._reject_floats(entity, values)
        self._reject_credentials(entity, values)

        ent = S.ENTITIES[entity]
        # The caller does not get to choose the organization. A create
        # that names another org is not an error to report back — it is
        # the request rewritten to the only org this scope has.
        if ent.org_col and ent.org_col != "id":
            values[ent.org_col] = self.scope.organization_id

        # A division-scoped creator may only create inside their own
        # divisions. Checked rather than assumed, because "the UI only
        # offers their divisions" stops being true the first time
        # somebody posts to the API directly.
        if ent.division_col and not self.scope.org_wide:
            divs = self.scope.divisions
            target = values.get(ent.division_col)
            if divs and target is not None and int(target) not in divs:
                raise RepositoryError(
                    f"cannot create a {entity} row in division {target}: "
                    f"this scope reaches {list(divs)}.")

        cols = list(values)
        cur = self.conn.cursor()
        cur.execute(
            f"INSERT INTO {entity} ({', '.join(cols)}) "
            f"VALUES ({', '.join(['%s'] * len(cols))}) RETURNING id",
            [values[c] for c in cols])
        new_id = cur.fetchone()[0]
        cur.close()

        A.record(self.conn, action="create", target_type=entity,
                 target_id=new_id, actor_user_id=self.scope.user_id,
                 detail={"columns": sorted(cols)},
                 request_id=self.request_id)
        return new_id

    def update(self, entity: str, row_id, values: dict) -> bool:
        """Update one row, only if it is inside the caller's scope.

        The scope predicate is part of the UPDATE's WHERE rather than a
        prior SELECT. A check-then-write leaves a window in which the row
        moves out of scope between the two, and more importantly it means
        the authorization and the write can drift apart in a later edit.
        """
        scoped = self._scoped(entity, "update", {"id": row_id})
        if scoped is None:
            return False
        where, params = scoped

        values = dict(values)
        if not values:
            return False
        self._check_columns(entity, values.keys())
        self._reject_floats(entity, values)
        self._reject_credentials(entity, values)

        sets = ", ".join(f"{c} = %s" for c in values)
        cur = self.conn.cursor()
        cur.execute(f"UPDATE {entity} SET {sets} WHERE {where}",
                    list(values.values()) + params)
        touched = cur.rowcount
        cur.close()

        if touched:
            A.record(self.conn, action="update", target_type=entity,
                     target_id=row_id, actor_user_id=self.scope.user_id,
                     detail={"columns": sorted(values)},
                     request_id=self.request_id)
        return bool(touched)

    def archive(self, entity: str, row_id) -> bool:
        """Soft-delete. There is no hard delete in this layer, on purpose.

        A deleted lease is a deleted defence in a dispute, and a deleted
        role assignment is a deleted answer to "who could see this in
        March". Rows go dark, not away.
        """
        if "archived_at" not in self.columns(entity):
            raise RepositoryError(
                f"{entity} has no archived_at column, so it cannot be "
                f"archived. Add one rather than reaching for DELETE.")
        scoped = self._scoped(entity, "archive", {"id": row_id})
        if scoped is None:
            return False
        where, params = scoped
        cur = self.conn.cursor()
        cur.execute(f"UPDATE {entity} SET archived_at = NOW() "
                    f"WHERE {where} AND archived_at IS NULL", params)
        touched = cur.rowcount
        cur.close()
        if touched:
            A.record(self.conn, action="archive", target_type=entity,
                     target_id=row_id, actor_user_id=self.scope.user_id,
                     request_id=self.request_id)
        return bool(touched)

    # ── role assignment, which is not an ordinary write ──
    def grant_role(self, user_id: int, role_key: str,
                   division_id: int | None = None,
                   property_id: int | None = None) -> int | None:
        """Give a user a role, refusing anything at or above the granter.

        This is a method rather than an insert() call because the rank
        rule is not expressible as a column filter, and because a role
        grant is the write most worth getting right: it is the one that
        changes what every later authorization decision returns.

        The database enforces the other half — 0001_foundation's
        mf_user_roles_scope_ck trigger refuses an unscoped grant for any
        role but platform_admin, so an org-wide division_manager cannot
        be written even by code that bypasses this method.
        """
        if not S.may_grant(self.scope, role_key):
            reason = (f"{role_key} ranks {S.ROLE_RANK[role_key]}, this scope "
                      f"ranks {self.scope.rank}")
            A.record_denied(self.conn, self.scope, "mf_user_roles",
                            "create", reason, self.request_id)
            log.info("denied grant of %s by user=%s: %s", role_key,
                     self.scope.user_id, reason)
            return None

        cur = self.conn.cursor()
        cur.execute("SELECT id FROM mf_roles WHERE key = %s", (role_key,))
        row = cur.fetchone()
        cur.close()
        if not row:
            raise RepositoryError(f"No such role {role_key!r}.")

        new_id = self.insert("mf_user_roles", {
            "user_id": user_id, "role_id": row[0],
            "division_id": division_id, "property_id": property_id,
            "granted_by": self.scope.user_id})
        if new_id is not None:
            # Every live session for that user is now stale. This is the
            # revocation path from 0001_foundation: bumping the epoch
            # kills them on their next request without a session scan.
            cur = self.conn.cursor()
            cur.execute("UPDATE mf_users SET privilege_epoch = "
                        "privilege_epoch + 1 WHERE id = %s", (user_id,))
            cur.close()
            A.record(self.conn, action="privilege_change",
                     target_type="mf_users", target_id=user_id,
                     actor_user_id=self.scope.user_id,
                     detail={"granted": role_key, "division_id": division_id,
                             "property_id": property_id},
                     request_id=self.request_id)
        return new_id

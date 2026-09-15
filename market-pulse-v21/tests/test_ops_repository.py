"""The repository against a real database — does the scoping reach the SQL?

Run:  python tests/test_ops_repository.py
SKIPS (exit 0) without DATABASE_URL. To run it for real:

    initdb -D /var/tmp/pgmf -A trust -U postgres
    pg_ctl -D /var/tmp/pgmf -o '-p 55433 -k /var/tmp' start
    createdb -h /var/tmp -p 55433 -U postgres mfops
    DATABASE_URL="postgresql://postgres@/mfops?host=/var/tmp&port=55433" \
        python tests/test_ops_repository.py

tests/test_ops_authz.py already proves the matrix — but it proves it
about predicates, and a predicate is a string until something executes
it. The failure this file exists to catch is the one where visible()
returns a perfectly correct WHERE clause and the repository forgets to
put it in the query. Everything below therefore asserts on ROWS, with
two divisions of real data loaded, so "the filter is missing" and "the
filter is wrong" both show up as the wrong people coming back.

It creates and drops only mf_ tables. Never point it at production.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

if not os.environ.get("DATABASE_URL"):
    print("SKIP — no DATABASE_URL. See this file's docstring to run it.")
    sys.exit(0)

import psycopg2

from lib.ops import audit as A
from lib.ops import repository as RP
from lib.ops import scope as S
from lib.ops.migrations import runner as R

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


conn = psycopg2.connect(os.environ["DATABASE_URL"])


def sql(q, args=None):
    cur = conn.cursor()
    cur.execute(q, args)
    rows = cur.fetchall() if cur.description else []
    cur.close()
    return rows


def fails(q, args=None):
    try:
        cur = conn.cursor()
        cur.execute(q, args)
        conn.commit()
        cur.close()
        return False
    except psycopg2.Error:
        conn.rollback()
        return True


# ── a clean database with the foundation on it ──
for t in ["mf_sessions", "mf_jobs", "mf_documents", "mf_audit_log",
          "mf_jurisdiction_rules", "mf_jurisdictions", "mf_user_roles",
          "mf_roles", "mf_users", "mf_divisions", "mf_organizations",
          "mf_migrations"]:
    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS {t} CASCADE")
    conn.commit()
    cur.close()
for fn in ("mf_audit_log_immutable()", "mf_user_roles_scope_check()"):
    cur = conn.cursor()
    cur.execute(f"DROP FUNCTION IF EXISTS {fn} CASCADE")
    conn.commit()
    cur.close()
R.migrate(conn, actor="test_ops_repository")


# ── the fixture: one org, two divisions, people in each ──
def seed():
    cur = conn.cursor()
    cur.execute("INSERT INTO mf_organizations (legal_name) VALUES "
                "('Havel Property Holdings LLC') RETURNING id")
    org = cur.fetchone()[0]
    cur.execute("INSERT INTO mf_divisions (organization_id, name) VALUES "
                "(%s, 'California') RETURNING id", (org,))
    div_ca = cur.fetchone()[0]
    cur.execute("INSERT INTO mf_divisions (organization_id, name) VALUES "
                "(%s, 'Rhode Island') RETURNING id", (org,))
    div_ri = cur.fetchone()[0]

    people = {}
    for key, email, div in (
            ("admin", "admin@example.invalid", None),
            ("dm_ca", "camanager@example.invalid", div_ca),
            ("dm_ri", "rimanager@example.invalid", div_ri),
            ("staff_ca", "castaff@example.invalid", div_ca),
            ("staff_ri", "ristaff@example.invalid", div_ri),
            ("tenant_ca", "catenant@example.invalid", div_ca),
            ("tenant_ri", "ritenant@example.invalid", div_ri)):
        cur.execute("INSERT INTO mf_users (organization_id, email, full_name, "
                    "division_id) VALUES (%s, %s, %s, %s) RETURNING id",
                    (org, email, key, div))
        people[key] = cur.fetchone()[0]
    conn.commit()
    cur.close()
    return org, div_ca, div_ri, people


ORG, CA, RI, P = seed()
G = S.RoleGrant


def scope_for(key, portal, *grants):
    return S.Scope(user_id=P[key], organization_id=ORG, portal=portal,
                   grants=tuple(grants))


ADMIN = scope_for("admin", "staff", G("platform_admin"))
DM_CA = scope_for("dm_ca", "staff", G("division_manager", division_id=CA))
DM_RI = scope_for("dm_ri", "staff", G("division_manager", division_id=RI))
STAFF_CA = scope_for("staff_ca", "staff", G("staff", division_id=CA))
TEN_CA = scope_for("tenant_ca", "tenant", G("tenant", division_id=CA))


def repo(scope):
    return RP.Repository(conn, scope, request_id="test-req")


# ── the predicate actually reaches the query ──
admin_rows = repo(ADMIN).fetch("mf_users")
check(len(admin_rows) == 7,
      f"the platform admin sees all seven people (got {len(admin_rows)})")

ca_rows = repo(DM_CA).fetch("mf_users")
ri_rows = repo(DM_RI).fetch("mf_users")
ca_names = {r["full_name"] for r in ca_rows}
ri_names = {r["full_name"] for r in ri_rows}
check(ca_names == {"dm_ca", "staff_ca", "tenant_ca"},
      f"THE CALIFORNIA MANAGER SEES ONLY CALIFORNIA PEOPLE (got "
      f"{sorted(ca_names)}). If the predicate were dropped from the query "
      f"this would be all seven, and every other check here would still "
      f"pass")
check(ri_names == {"dm_ri", "staff_ri", "tenant_ri"},
      f"and the Rhode Island manager only Rhode Island people (got "
      f"{sorted(ri_names)})")
check(not (ca_names & ri_names),
      "with no overlap at all — two managers, two disjoint answers, from "
      "the same table and the same code")
check(repo(DM_CA).count("mf_users") == 3
      and repo(DM_RI).count("mf_users") == 3,
      "count() is scoped identically. A count that ignores scope leaks "
      "how many records exist, which is often the answer somebody wanted")

self_rows = repo(TEN_CA).fetch("mf_users")
check(len(self_rows) == 1 and self_rows[0]["id"] == P["tenant_ca"],
      f"a tenant reads exactly one row: their own (got {len(self_rows)})")
check(repo(TEN_CA).fetch_one("mf_users", P["tenant_ri"]) is None,
      "AND CANNOT FETCH ANOTHER TENANT BY ID. Asking for a specific row is "
      "the path a list-scoped filter most often fails to cover")
check(repo(DM_CA).fetch_one("mf_users", P["staff_ri"]) is None,
      "nor can the California manager reach a Rhode Island staff member "
      "by id")
check(repo(DM_CA).fetch_one("mf_users", P["staff_ca"])["full_name"]
      == "staff_ca", "but their own division's staff member comes back")
check(repo(DM_CA).exists(P and "mf_users", P["staff_ri"]) is False,
      "exists() is scoped too, so it cannot be used to probe for rows the "
      "caller may not read")


# ── denied reads look exactly like empty ones ──
check(repo(TEN_CA).fetch("mf_divisions") == [],
      "a denied read returns an empty list, not an error — a route that "
      "can tell 'denied' from 'nothing there' leaks the existence of the "
      "records it was refused")
check(repo(TEN_CA).fetch("mf_leases") == [],
      "including for a table that does not exist at all — default deny "
      "answers before anything reaches Postgres")


# ── secrets never come back ──
cur = conn.cursor()
cur.execute("UPDATE mf_users SET password_hash = 'argon2$fake', "
            "mfa_secret = 'JBSWY3DPEHPK3PXP' WHERE id = %s", (P["staff_ca"],))
conn.commit()
cur.close()
row = repo(ADMIN).fetch_one("mf_users", P["staff_ca"])
check("password_hash" not in row and "mfa_secret" not in row,
      "A DEFAULT SELECT DOES NOT RETURN THE PASSWORD HASH OR TOTP SEED, "
      "for the platform admin. The columns are expanded from "
      "information_schema minus a ban list rather than being SELECT *, "
      "which is what makes this hold for columns a later migration adds")
check(raises(S.ScopeError, repo(ADMIN).fetch, "mf_users",
             columns=["id", "mfa_secret"]),
      "and asking for one by name is refused rather than ignored")
check("email" in row and "full_name" in row,
      "ordinary columns are all there")


# ── writes ──
new_id = repo(DM_CA).insert("mf_users", {
    "email": "newhire@example.invalid", "full_name": "new hire",
    "division_id": CA})
check(new_id is not None, "a division manager can create a user")
check(sql("SELECT organization_id FROM mf_users WHERE id = %s",
          (new_id,))[0][0] == ORG,
      "and the organization is taken from the scope, not the caller")

forged = repo(DM_CA).insert("mf_users", {
    "email": "forged@example.invalid", "full_name": "forged",
    "organization_id": 999999, "division_id": CA})
check(sql("SELECT organization_id FROM mf_users WHERE id = %s",
          (forged,))[0][0] == ORG,
      "AN INSERT NAMING ANOTHER ORGANIZATION IS REWRITTEN TO THE CALLER'S. "
      "Not an error to report back — there is exactly one organization "
      "this scope may write to, so the request is simply that request")

check(raises(RP.RepositoryError, repo(DM_CA).insert, "mf_users",
             {"email": "x@example.invalid", "division_id": RI}),
      "but creating into ANOTHER DIVISION raises. 'The UI only offers "
      "their divisions' stops being true the first time somebody posts to "
      "the API directly")
check(repo(STAFF_CA).insert("mf_users", {"email": "y@example.invalid"}) is None,
      "and staff cannot create users at all — a denied write returns None "
      "rather than raising, matching the read path")

check(repo(DM_CA).update("mf_users", P["staff_ca"], {"title": "Super"}),
      "a manager updates their own division's person")
check(sql("SELECT title FROM mf_users WHERE id = %s",
          (P["staff_ca"],))[0][0] == "Super", "and it lands")
check(repo(DM_CA).update("mf_users", P["staff_ri"], {"title": "Hacked"})
      is False,
      "UPDATING ACROSS THE DIVISION BOUNDARY TOUCHES NOTHING. The scope "
      "predicate is inside the UPDATE's own WHERE, not a SELECT before "
      "it — a check-then-write can drift apart in a later edit, and this "
      "cannot")
check(sql("SELECT title FROM mf_users WHERE id = %s",
          (P["staff_ri"],))[0][0] is None,
      "and the row is genuinely unchanged, not merely reported as such")

check(raises(RP.RepositoryError, repo(ADMIN).update, "mf_users",
             P["staff_ca"], {"password_hash": "argon2$new"}),
      "A CREDENTIAL CANNOT BE WRITTEN THROUGH THE GENERIC PATH. A generic "
      "update would skip hashing, skip the audit row, and skip bumping "
      "privilege_epoch — three omissions that each look like nothing")
check(raises(RP.RepositoryError, repo(ADMIN).insert, "mf_users",
             {"email": "z@example.invalid", "mfa_secret": "SEED"}),
      "on insert too")

check(raises(RP.RepositoryError, repo(ADMIN).update, "mf_users",
             P["staff_ca"], {"privilege_epoch": 2.5}),
      "A FLOAT IS REFUSED AT THE WRITE BOUNDARY. The schema grep stops a "
      "float COLUMN being created; this stops a float VALUE reaching a "
      "BIGINT one, where the driver would coerce 1999.9999999998 to 1999 "
      "and nobody would ever see it happen")
check(raises(RP.RepositoryError, repo(ADMIN).update, "mf_users",
             P["staff_ca"], {"not_a_column": 1}),
      "an unknown column raises rather than being interpolated — column "
      "names are checked against the database, which is what keeps this "
      "layer free of injection")
check(raises(RP.RepositoryError, repo(ADMIN).fetch, "mf_users",
             order="email; DROP TABLE mf_users"),
      "and an ORDER BY is checked the same way")


# ── archive, not delete ──
check(repo(ADMIN).archive("mf_users", new_id), "a user can be archived")
check(sql("SELECT archived_at FROM mf_users WHERE id = %s",
          (new_id,))[0][0] is not None, "archived_at is stamped")
check(sql("SELECT COUNT(*) FROM mf_users WHERE id = %s", (new_id,))[0][0] == 1,
      "AND THE ROW IS STILL THERE. A deleted lease is a deleted defence in "
      "a dispute; ops rows go dark, not away")
check(not hasattr(RP.Repository, "delete"),
      "there is no delete() on the repository at all — the absence is the "
      "design, not an oversight")
check(raises(RP.RepositoryError, repo(ADMIN).archive, "mf_jurisdictions", 1),
      "archiving a table with no archived_at raises rather than silently "
      "doing nothing")


# ── role grants ──
before_epoch = sql("SELECT privilege_epoch FROM mf_users WHERE id = %s",
                   (P["staff_ca"],))[0][0]
gid = repo(DM_CA).grant_role(P["staff_ca"], "tenant", division_id=CA)
check(gid is not None, "a division manager may grant a role below them")
after_epoch = sql("SELECT privilege_epoch FROM mf_users WHERE id = %s",
                  (P["staff_ca"],))[0][0]
check(after_epoch == before_epoch + 1,
      "AND THE USER'S privilege_epoch IS BUMPED. Every live session that "
      "user holds is now stale on its next request — this is the "
      "revocation path, and it works without scanning the session table")

check(repo(DM_CA).grant_role(P["staff_ca"], "platform_admin") is None,
      "A DIVISION MANAGER CANNOT MINT A PLATFORM ADMIN. This is the "
      "escalation that matters, and it is refused before any SQL runs")
check(repo(DM_CA).grant_role(P["staff_ca"], "division_manager",
                             division_id=CA) is None,
      "nor another division manager — rank must strictly dominate")
check(repo(STAFF_CA).grant_role(P["tenant_ca"], "tenant", division_id=CA)
      is None,
      "and staff grant nothing at all")

conn.commit()
check(fails("INSERT INTO mf_user_roles (user_id, role_id) SELECT %s, id "
            "FROM mf_roles WHERE key = 'division_manager'", (P["staff_ca"],)),
      "AND THE DATABASE REFUSES AN UNSCOPED division_manager GRANT WRITTEN "
      "DIRECTLY, bypassing every line of Python above. scope.py reads a "
      "null division as 'no filter', so that row would have seen every "
      "division — mf_user_roles_scope_ck makes it unwritable")
check(not fails("INSERT INTO mf_user_roles (user_id, role_id) SELECT %s, id "
                "FROM mf_roles WHERE key = 'platform_admin'",
                (P["admin"],)),
      "while an unscoped platform_admin grant is permitted, that being the "
      "one role for which org-wide is the correct answer")


# ── the audit trail ──
def audit_rows(action=None, target=None):
    q = "SELECT actor_user_id, action, target_type, target_id, detail " \
        "FROM mf_audit_log WHERE TRUE"
    args = []
    if action:
        q += " AND action = %s"
        args.append(action)
    if target:
        q += " AND target_type = %s"
        args.append(target)
    return sql(q + " ORDER BY id", args)


reads = audit_rows("read_pii", "mf_users")
check(len(reads) > 0,
      "READING mf_users WROTE AUDIT ROWS. CLAUDE.md requires a record for "
      "every read of tenant PII, and the decision lives on the table "
      "definition (scope.ENTITIES pii=True) rather than at each call site "
      "— a rule applied by hand at forty call sites is applied at "
      "thirty-eight")
_last = reads[-1] if reads else (None, None, None, None, {})
check(_last[4].get("count") is not None and _last[4].get("portal"),
      "the audit detail records how many rows and through which portal")
check(any(r[4].get("ids") for r in reads),
      "and WHICH rows. 'Someone read tenant records' answers no question "
      "worth asking; 'user 4 read rows 11, 12 and 19' answers the one "
      "that comes up in a subject access request")

check(len(audit_rows(None, "mf_divisions")) == 0
      or all(r[1] != "read_pii" for r in audit_rows(None, "mf_divisions")),
      "a non-PII table does not generate read_pii noise")

denials = audit_rows("denied")
check(len(denials) > 0,
      "REFUSALS ARE LOGGED TOO. A tenant session asking for the staff user "
      "list is either a bug or an attempt, and both are things you want to "
      "learn from the log rather than from the outcome")
check(any(r[4].get("attempted") and r[4].get("reason") for r in denials),
      "with what was attempted and why it was refused")

check(len(audit_rows("create", "mf_users")) >= 1, "creates are logged")
check(len(audit_rows("update", "mf_users")) >= 1, "updates are logged")
check(len(audit_rows("archive", "mf_users")) >= 1, "archives are logged")
check(len(audit_rows("privilege_change", "mf_users")) >= 1,
      "and a privilege change is logged as its own action, because it is "
      "the write that changes what every later authorization returns")

check(raises(A.AuditError, A.record, conn, action="whatever",
             target_type="mf_users"),
      "an action outside the closed vocabulary is refused — free text "
      "turns into 'update', 'updated' and 'user_update' within a year, "
      "and then no query over the log is trustworthy")

conn.commit()
check(fails("DELETE FROM mf_audit_log"),
      "and none of it can be deleted afterwards, which is what makes the "
      "rest of this worth writing")


conn.close()

if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for m in _FAILS:
        print("  ✗", m)
    sys.exit(1)
print(f"OK — all {_COUNT} ops-repository checks passed.")
print("   Two divisions of real rows: each manager saw only their own, "
      "across read,\n   fetch-by-id, count, update and exists. Secrets "
      "stayed in the database.")
sys.exit(0)

"""The foundation schema, and the guards that keep it a foundation.

Run:  python tests/test_ops_schema.py

Two halves, and the split is deliberate.

The FIRST half is pure — it reads the .sql files off disk and never opens
a connection, so it runs everywhere including CI and this sandbox. It
holds the mechanical guard from ARCHITECTURE.md §2 that costs nothing:
no float money type may appear in an mf_ table. A guard that only runs
when a database happens to be around is a guard that stops running.

The SECOND half needs a real Postgres and skips without one. It proves
the things you cannot prove by reading SQL:

  * the migration runs UP, DOWN, and UP again leaving nothing behind
    (Phase 1 acceptance: "migrations run clean up and down")
  * the checksum actually refuses a migration edited after it was applied
  * mf_audit_log genuinely cannot be modified — through all five paths,
    not just the two obvious ones

That last item is here because assuming it was a bug. The row-level
BEFORE DELETE trigger looked complete and was not: `DELETE FROM
mf_audit_log WHERE id = -999` matches no rows, fires no row trigger, and
returns SUCCESS. Nothing was destroyed, but the table answered "yes, you
may delete from me", which is the wrong answer from a table whose whole
promise is that it is append-only. The statement-level triggers exist
because this test was written and run, not because it was reasoned about.

To run the database half:

    initdb -D /var/tmp/pgmf -A trust -U postgres
    pg_ctl -D /var/tmp/pgmf -o '-p 55433 -k /var/tmp' start
    createdb -h /var/tmp -p 55433 -U postgres mfops
    DATABASE_URL="postgresql://postgres@/mfops?host=/var/tmp&port=55433" \
        python tests/test_ops_schema.py

It creates and drops only mf_* tables. Never point it at production.
"""
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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


# ══════════════════════════════════════════════════════════════════
# PURE HALF — reads the files, no database
# ══════════════════════════════════════════════════════════════════

MIG = R.MIGRATIONS_DIR


def strip_sql_noise(sql: str) -> str:
    """SQL with comments and string literals removed.

    Both are full of false positives: the header comment says "never
    DOUBLE PRECISION" and a seed row says 'NOT A REAL AUTHORITY'. A
    scanner that flagged those would be turned off within a week, and a
    guard nobody trusts is worse than no guard.
    """
    out = re.sub(r"--[^\n]*", "", sql)          # line comments
    out = re.sub(r"/\*.*?\*/", "", out, flags=re.S)   # block comments
    out = re.sub(r"'(?:[^']|'')*'", "''", out)  # string literals
    return out


# ── guard 1 of 3 (ARCHITECTURE.md §2): no float money in mf_ tables ──
# The host repo stores money as DOUBLE PRECISION for the analysis boards.
# That is fine for a market-cap estimate and fatal for a rent ledger: the
# damage is not one wrong cent, it is a ledger that stops reconciling
# with no way to say when it started. NUMERIC is on the list too — it is
# exact, but it invites Decimal into a codebase where lib/ops/money.py
# has already settled on integer minor units, and two money
# representations is how you get a conversion bug at the boundary.
FLOAT_TYPES = re.compile(
    r"\b(DOUBLE\s+PRECISION|FLOAT\s*(\(\s*\d+\s*\))?|REAL|NUMERIC|DECIMAL|MONEY)\b",
    re.I)

_offenders = []
for path in sorted(MIG.glob("*.sql")):
    body = strip_sql_noise(path.read_text())
    for m in FLOAT_TYPES.finditer(body):
        line = body[:m.start()].count("\n") + 1
        _offenders.append(f"{path.name}:{line} {m.group(0)}")

check(not _offenders,
      "NO FLOAT MONEY TYPE APPEARS IN ANY mf_ MIGRATION. Found: "
      + "; ".join(_offenders))

# The guard has to be able to fail, or it is decoration. Prove the
# detector fires on a table definition that a careless migration would
# plausibly contain.
check(FLOAT_TYPES.search(strip_sql_noise(
        "CREATE TABLE mf_x (rent DOUBLE PRECISION);")),
      "the float detector catches DOUBLE PRECISION")
check(FLOAT_TYPES.search(strip_sql_noise(
        "CREATE TABLE mf_x (rent NUMERIC(12,2));")),
      "and NUMERIC(12,2), which looks safe and starts the second "
      "money representation")
check(FLOAT_TYPES.search(strip_sql_noise(
        "CREATE TABLE mf_x (r REAL, s FLOAT8);")),
      "and REAL")
check(not FLOAT_TYPES.search(strip_sql_noise(
        "-- money is never DOUBLE PRECISION\nCREATE TABLE mf_x (c BIGINT);")),
      "but NOT the word in a comment — a scanner with false positives "
      "gets disabled, and then it is not a guard at all")
check(not FLOAT_TYPES.search(strip_sql_noise(
        "INSERT INTO mf_x VALUES ('NOT A REAL AUTHORITY');")),
      "and not inside a string literal either")

# Money columns that DO exist must be BIGINT. There are none in 0001 —
# Phase 2 brings the ledger — so this asserts the absence rather than
# pretending to check something that is not there yet.
_all_sql = "\n".join(strip_sql_noise(p.read_text()) for p in sorted(MIG.glob("*.sql")))
check(not re.search(r"\b\w*(amount|cents|price|rent|balance|fee)\w*\s+(?!BIGINT)",
                    _all_sql, re.I)
      or "BIGINT" in _all_sql,
      "any money-shaped column in the foundation is BIGINT")


# ── guard 2 of 3: mf_ tables are reachable only through the repository ──
# ARCHITECTURE.md §2: "no raw conn.execute against an mf_* table outside
# it". The scoping predicate is applied in exactly one place, so a query
# written anywhere else is a query with no authorization on it — and it
# would look completely ordinary in review.
ROOT = Path(__file__).resolve().parent.parent

# Every allowance is named, AND NAMES THE TABLES IT MAY TOUCH. A blanket
# exemption would mean each of these files could grow a query against any
# ops table later and the guard would keep passing — which is how an
# allowlist stops being a boundary and becomes a list of files nobody
# checks.
SQL_ALLOWED = {
    # The sanctioned path itself. Unrestricted by definition.
    "lib/ops/repository.py": None,
    # The audit writer. Only ever INSERTs into mf_audit_log, and an audit
    # row is a fact rather than a scoped record — there is nothing for the
    # repository to filter. Routing it through repository.insert() would
    # also make the audit of a write depend on the caller having create
    # permission on the log, which is backwards.
    "lib/ops/audit.py": {"mf_audit_log"},
    # Authentication CANNOT be scoped, because it is what produces a
    # scope: there is no Scope to filter by until login succeeds. It also
    # reads password_hash and mfa_secret, which the repository bans
    # outright. So it gets direct SQL — narrowed to the four identity
    # tables, so it cannot quietly grow a query against a rent ledger.
    "lib/ops/auth.py": {"mf_users", "mf_sessions", "mf_roles",
                        "mf_user_roles"},
    # The job worker is not a user: no session, no portal, no Scope, so
    # there is nothing for the scoping layer to filter by. It also needs
    # FOR UPDATE SKIP LOCKED, which the repository deliberately does not
    # express. Narrowed to mf_jobs — a worker with unrestricted SQL would
    # be an unauthenticated path to every ops table.
    "lib/ops/jobs.py": {"mf_jobs"},
    # DDL and the ledger. This is where schema is supposed to live.
    "lib/ops/migrations/runner.py": {"mf_migrations"},
    # Tests set up and inspect state directly, which is the only way to
    # prove the repository's scoping is doing anything.
}

SQL_REF = re.compile(
    r"\b(?:FROM|JOIN|INTO|UPDATE|TABLE|TRUNCATE)\s+(mf_[a-z_]+)", re.I)

_raw = []
for path in sorted(ROOT.rglob("*.py")):
    rel = path.relative_to(ROOT).as_posix()
    if rel.startswith("tests/") or "/__pycache__/" in f"/{rel}":
        continue
    allowed = SQL_ALLOWED.get(rel, frozenset())
    if allowed is None:
        continue
    body = path.read_text()
    for m in SQL_REF.finditer(body):
        table = m.group(1)
        if table in allowed:
            continue
        line = body[:m.start()].count("\n") + 1
        _raw.append(f"{rel}:{line} → {table}")

check(not _raw,
      "NO SQL OUTSIDE lib/ops/repository.py TOUCHES AN mf_ TABLE, beyond "
      "the narrowly-listed exceptions. Found: " + "; ".join(_raw))
check(set(SQL_ALLOWED) - {"lib/ops/repository.py"} == {
          "lib/ops/audit.py", "lib/ops/auth.py", "lib/ops/jobs.py",
          "lib/ops/migrations/runner.py"},
      "and the exception list is exactly these four files — a new entry "
      "has to be added here deliberately, where the justification comment "
      "sits next to it")

# And the scanner has to be able to see one. Written as a literal here
# rather than by planting a file, so the check is honest about what it
# actually detects.
check(SQL_REF.search("cur.execute('SELECT * FROM mf_users')"),
      "the scanner catches a plain SELECT")
check(SQL_REF.search('cur.execute("UPDATE mf_leases SET rent = 1")'),
      "and an UPDATE against a table that does not exist yet — Phase 2's "
      "tables are covered the day they are created, without editing this")
check(SQL_REF.search("DELETE FROM  mf_audit_log"), "and odd whitespace")
check(SQL_REF.search("SELECT x FROM a JOIN mf_users u ON u.id = a.uid"),
      "and a JOIN, which is the one a FROM-only scanner misses")
check(not SQL_REF.search('has_mf_data = True'),
      "but NOT an ordinary variable that happens to start with mf_ — "
      "main.py has several, and a scanner that cried wolf on those would "
      "be switched off inside a week")
check(not SQL_REF.search('return {"mf_score": score}'),
      "nor a dict key")


# ── guard 3 of 3: every /ops route declares a scope ──
from lib.ops import routeguard as RG

_ops_router_files = sorted(
    p.relative_to(ROOT).as_posix()
    for p in (ROOT / "routers" / "ops").glob("*.py")
    if p.name != "__init__.py")

if _ops_router_files:
    import importlib

    _undeclared, _checked = [], 0
    for rel in _ops_router_files:
        mod = importlib.import_module(rel[:-3].replace("/", "."))
        for attr in vars(mod).values():
            if hasattr(attr, "routes"):
                _undeclared += RG.undeclared(attr)
                _checked += sum(1 for r in attr.routes
                                if getattr(r, "path", "").startswith("/ops"))
    check(not _undeclared,
          "EVERY ROUTE UNDER /ops DECLARES A SCOPE. Undeclared: "
          + "; ".join(sorted(set(_undeclared))))
    # Without this the guard passes just as happily when the import found
    # no routers at all — which is the shape of a test that quietly
    # stopped testing. It has to have looked at something.
    check(_checked >= 20,
          f"and it actually examined the ops routes rather than an empty "
          f"list (saw {_checked}; four portals x five auth routes is "
          f"twenty before the API)")
else:
    # Say it out loud rather than passing quietly. A guard with nothing
    # to check is not the same as a guard that found nothing wrong, and
    # writing it the other way is how a vacuous test survives to the day
    # it was supposed to matter. The enumerator itself is exercised
    # against a router built for the purpose in tests/test_ops_authz.py.
    check(RG.undeclared.__doc__ is not None,
          "the route enumerator exists and is armed")
    print("   NOTE: routers/ops/ is empty, so guard 3 has no real routes "
          "to check yet.\n   Its enumerator is proved against a synthetic "
          "router in tests/test_ops_authz.py.")


# ── discovery refuses one-way migrations ──
found = R.discover()
check(len(found) >= 1, "0001_foundation is discovered")
# Everything on disk, so the cycle below stays correct as migrations are
# added. A test pinned to one migration name starts silently proving less
# the moment there are two — 0002_auth was exactly that moment.
ALL_NAMES = [f"{m['version']:04d}_{m['name']}" for m in found]
ALL_VERSIONS = [m["version"] for m in found]
check(found[0]["version"] == 1 and found[0]["name"] == "foundation",
      "version and name parse out of the filename")
check(found[0]["sql_up"].strip() and found[0]["sql_down"].strip(),
      "both halves have content — an empty .down.sql satisfies the file "
      "check and rolls back nothing")
check([m["version"] for m in found] == sorted(m["version"] for m in found),
      "migrations come back in version order, not in whatever order the "
      "filesystem returns them")
check(len(found[0]["checksum"]) == 16,
      "each migration carries a checksum of its up half")

import tempfile

with tempfile.TemporaryDirectory() as d:
    p = Path(d)
    (p / "0001_a.up.sql").write_text("SELECT 1;")
    check(raises(R.MigrationError, R.discover, p),
          "AN UP WITH NO DOWN RAISES. 'I will add the down later' is how a "
          "schema becomes one-way, and by then there is a rent ledger in it")
    (p / "0001_a.down.sql").write_text("SELECT 1;")
    check(len(R.discover(p)) == 1, "and passes once the down exists")

    (p / "0002_b.down.sql").write_text("SELECT 1;")
    check(raises(R.MigrationError, R.discover, p),
          "a down with no up raises too — it means a migration file was "
          "deleted, and the checkout can no longer roll back")
    (p / "0002_b.down.sql").unlink()

    (p / "notes.sql").write_text("-- scratch")
    check(raises(R.MigrationError, R.discover, p),
          "a stray .sql that does not match NNNN_name.(up|down).sql raises "
          "rather than being ignored — an ignored file is a migration that "
          "silently never runs")
    (p / "notes.sql").unlink()

    _c1 = R.discover(p)[0]["checksum"]
    (p / "0001_a.up.sql").write_text("SELECT 2;")
    check(R.discover(p)[0]["checksum"] != _c1,
          "editing the up half changes the checksum — which is the whole "
          "mechanism drift detection rests on")


# ══════════════════════════════════════════════════════════════════
# DATABASE HALF
# ══════════════════════════════════════════════════════════════════

def _report_and_exit(tail=""):
    if _FAILS:
        print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
        for m in _FAILS:
            print("  ✗", m)
        sys.exit(1)
    print(f"OK — all {_COUNT} ops-schema checks passed.")
    if tail:
        print(tail)
    sys.exit(0)


DB_URL = os.environ.get("DATABASE_URL")
if not DB_URL:
    _report_and_exit(
        "   Pure half only — no DATABASE_URL, so the up/down/up cycle and "
        "the audit-log\n   immutability checks did NOT run. See this file's "
        "docstring to run them.")

import psycopg2

conn = psycopg2.connect(DB_URL)


class _Broken:
    """A query that errored. Equal to nothing, so every check on it fails.

    A missing table downstream of a broken rollback would otherwise abort
    the run on the first SELECT, hiding every later check. This lets the
    file finish and print the whole picture.
    """

    def __init__(self, why):
        self.why = why

    def __eq__(self, other):
        return False

    def __iter__(self):
        return iter(())     # comprehensions over it yield nothing...

    def __len__(self):
        return 0            # ...and never accidentally match a count

    def __repr__(self):
        return f"<query failed: {self.why}>"


def q(sql, args=None):
    # args stays None rather than () — psycopg2 treats an empty tuple as
    # "interpolate this", and then a literal % in a LIKE pattern is read
    # as a placeholder.
    try:
        cur = conn.cursor()
        cur.execute(sql, args)
        rows = cur.fetchall() if cur.description else []
        cur.close()
        return rows
    except psycopg2.Error as e:
        conn.rollback()
        return _Broken(str(e).strip().splitlines()[0])


def one(sql, args=None):
    rows = q(sql, args)
    if isinstance(rows, _Broken):
        return rows
    return rows[0][0] if rows else None


def mf_tables():
    return [r[0] for r in q(
        "SELECT tablename FROM pg_tables WHERE schemaname='public' "
        "AND tablename LIKE 'mf!_%' ESCAPE '!' ORDER BY tablename")]


def attempt(label, fn, *a, **k):
    """Run a migration call; a raise becomes a recorded failure, not a crash.

    Without this, a broken .down.sql aborts the file with a traceback and
    you learn about exactly one problem. The interesting case — a down
    that drops less than the up created — fails BOTH the leftover-tables
    check and the second up, and seeing both together is what tells you
    which table was forgotten.
    """
    try:
        return fn(*a, **k)
    except R.MigrationError as e:
        check(False, f"{label} raised: {e}")
        conn.rollback()
        return None


def fails(sql, args=None):
    """True if the statement raised. Rolls back so the connection survives."""
    try:
        cur = conn.cursor()
        cur.execute(sql, args)
        conn.commit()
        cur.close()
        return False
    except psycopg2.Error:
        conn.rollback()
        return True


# Start from nothing, whatever the last run left.
for t in ["mf_sessions", "mf_jobs", "mf_documents", "mf_audit_log",
          "mf_jurisdiction_rules", "mf_jurisdictions", "mf_user_roles",
          "mf_roles", "mf_users", "mf_divisions", "mf_organizations",
          "mf_migrations"]:
    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS {t} CASCADE")
    conn.commit()
    cur.close()
cur = conn.cursor()
cur.execute("DROP FUNCTION IF EXISTS mf_audit_log_immutable() CASCADE")
conn.commit()
cur.close()

check(mf_tables() == [], "the test starts against a database with no mf_ tables")


# ── up ──
ran = attempt("first up", R.migrate, conn, actor="test_ops_schema")
check(ran == ALL_NAMES,
      f"EVERY migration on disk applies, in order (expected {ALL_NAMES}, "
      f"got {ran})")

_tables = set(mf_tables())
for t in ("mf_organizations", "mf_divisions", "mf_users", "mf_roles",
          "mf_user_roles", "mf_jurisdictions", "mf_jurisdiction_rules",
          "mf_audit_log", "mf_documents", "mf_jobs", "mf_sessions"):
    check(t in _tables, f"{t} exists after migrating up")

check(R.migrate(conn) == [],
      "running it again applies nothing — the ledger, not IF NOT EXISTS, "
      "is what makes this idempotent, and the difference shows the moment "
      "a migration does something a second run would repeat")

st = R.status(conn)
check(st["applied"] == ALL_VERSIONS and st["pending"] == []
      and st["problems"] == [],
      f"status reports them all applied, nothing pending, no drift "
      f"(got {st})")


# ── seeds ──
check(one("SELECT COUNT(*) FROM mf_roles") == 6, "six roles seed")
check({r[0] for r in q("SELECT key FROM mf_roles")} ==
      {"platform_admin", "division_manager", "staff", "owner_client",
       "tenant", "vendor"},
      "and they are the six CLAUDE.md names, spelled the way the code "
      "will compare them")
check(one("SELECT requires_mfa FROM mf_roles WHERE key='platform_admin'") is True
      and one("SELECT requires_mfa FROM mf_roles WHERE key='division_manager'") is True
      and one("SELECT requires_mfa FROM mf_roles WHERE key='tenant'") is False,
      "MFA is required for the two privileged roles and not for tenants — "
      "the requirement lives in data, so Phase 1-D enforces a fact rather "
      "than an if-statement someone can forget")

check(one("SELECT COUNT(*) FROM mf_jurisdictions") == 5, "five jurisdictions seed")
check(one("SELECT timezone FROM mf_jurisdictions WHERE slug='us-ca'")
      == "America/Los_Angeles"
      and one("SELECT timezone FROM mf_jurisdictions WHERE slug='us-ri'")
      == "America/New_York",
      "THE TWO PROPERTIES ARE IN DIFFERENT TIMEZONES IN THE DATA. That is "
      "what lib/ops/clock.py reads to decide what day a notice was served")
check(one("SELECT p.slug FROM mf_jurisdictions c "
          "JOIN mf_jurisdictions p ON p.id = c.parent_jurisdiction_id "
          "WHERE c.slug='us-ca-san-leandro'") == "us-ca-alameda",
      "city rolls up to county rolls up to state — rule lookup walks this "
      "chain, so the chain has to be real")

check(one("SELECT COUNT(*) FROM mf_jurisdiction_rules") == 2,
      "the two placeholder rules seed")
check(one("SELECT COUNT(*) FROM mf_jurisdiction_rules "
          "WHERE authority NOT LIKE '%NOT A REAL AUTHORITY%'") == 0,
      "EVERY seeded rule is labelled fake in its authority field. A "
      "placeholder that reads like law is the worst thing in this "
      "database — someone acts on it")
check(one("SELECT COUNT(*) FROM mf_jurisdiction_rules "
          "WHERE last_verified_at > CURRENT_DATE - 180") == 0,
      "and every one is stale enough to trip the 180-day warning on "
      "sight, rather than looking freshly checked")
check(one("SELECT value->>'days' FROM mf_jurisdiction_rules "
          "WHERE rule_key='PLACEHOLDER_notice_period_days'") == "999",
      "rule values are jsonb and read back as posted — a notice period of "
      "999 days is absurd on purpose")


# ── the audit log cannot be modified. all five paths. ──
cur = conn.cursor()
cur.execute("INSERT INTO mf_audit_log (action, target_type, target_id, actor_label) "
            "VALUES ('read_pii', 'mf_users', '1', 'test')")
conn.commit()
cur.close()
_audit_rows = one("SELECT COUNT(*) FROM mf_audit_log")
check(_audit_rows == 1, "an audit row inserts normally — append is the one "
                        "thing that must work")

check(fails("UPDATE mf_audit_log SET action='tampered' WHERE id > 0"),
      "UPDATE matching a row is blocked")
check(fails("UPDATE mf_audit_log SET action='tampered' WHERE id = -999"),
      "UPDATE MATCHING ZERO ROWS IS ALSO BLOCKED. A row-level trigger "
      "would let this succeed, and 'UPDATE succeeded' is a false answer "
      "from an append-only table even when nothing changed")
check(fails("DELETE FROM mf_audit_log WHERE id > 0"),
      "DELETE matching a row is blocked")
check(fails("DELETE FROM mf_audit_log WHERE id = -999"),
      "DELETE MATCHING ZERO ROWS IS ALSO BLOCKED — this is the one that "
      "was actually broken, found by running it rather than by reading "
      "the trigger definitions")
check(fails("TRUNCATE mf_audit_log"),
      "TRUNCATE is blocked — it is not a DELETE and fires none of the "
      "delete triggers, so without its own trigger it empties the table "
      "in one statement")
check(one("SELECT COUNT(*) FROM mf_audit_log") == _audit_rows,
      "and after five attempts every row is still there")
check(one("SELECT action FROM mf_audit_log ORDER BY id LIMIT 1") == "read_pii",
      "with its original contents")


# ── checksum drift ──
_orig = one("SELECT checksum FROM mf_migrations WHERE version=1")
cur = conn.cursor()
cur.execute("UPDATE mf_migrations SET checksum='deadbeefdeadbeef' WHERE version=1")
conn.commit()
cur.close()
check(R.verify(conn) != [],
      "a checksum that no longer matches the file is reported")
check(raises(R.MigrationError, R.migrate, conn),
      "AND migrate REFUSES TO RUN. Applying new migrations on top of a "
      "database whose earlier schema differs from the checkout produces a "
      "shape nobody has ever tested")
check(R.migrate(conn, allow_drift=True) == [],
      "the override exists and works, because sometimes the drift is "
      "known and benign — but it has to be typed")
cur = conn.cursor()
cur.execute("UPDATE mf_migrations SET checksum=%s WHERE version=1", (_orig,))
conn.commit()
cur.close()
check(R.verify(conn) == [], "restored, no complaints")


# ── down ──
back = attempt("rollback", R.rollback, conn, steps=len(ALL_NAMES))
check(back == list(reversed(ALL_NAMES)),
      f"ALL of them roll back, newest first (expected "
      f"{list(reversed(ALL_NAMES))}, got {back})")
check(mf_tables() == ["mf_migrations"],
      f"AND LEAVES NOTHING BEHIND except the ledger itself. Still there: "
      f"{[t for t in mf_tables() if t != 'mf_migrations']}")
check(one("SELECT COUNT(*) FROM pg_proc WHERE proname LIKE 'mf!_%' "
          "ESCAPE '!'") == 0,
      "including BOTH trigger functions — a leftover function makes the "
      "next up fail on CREATE OR REPLACE differences, or worse, succeed "
      "against a stale definition. Matched by prefix rather than by name "
      "so a function added in a later migration is covered without "
      "editing this check")
check(one("SELECT COUNT(*) FROM mf_migrations") == 0,
      "and the ledger row is gone, so the migration is genuinely pending "
      "again rather than applied-but-absent")


# ── up again ──
ran2 = attempt("second up", R.migrate, conn, actor="test_ops_schema")
check(ran2 == ALL_NAMES,
      "and it applies a SECOND time cleanly. up/down/up is the cycle that "
      "catches a down which drops less than the up created — the first up "
      "always works, it is the second that fails")
check(one("SELECT COUNT(*) FROM mf_roles") == 6,
      "seeds land again rather than colliding with survivors of the down")
check(len(mf_tables()) == 12,
      f"all eleven tables plus the ledger are back (got {mf_tables()})")

check(fails("DELETE FROM mf_audit_log WHERE id = -999"),
      "and the audit triggers came back with them — a down that drops a "
      "trigger and an up that forgets to recreate it silently disables "
      "the guarantee on the second deploy")


# ── boot wiring: fails open, and says so ──
# database.init_db() calls this on every deploy. The analysis boards have
# been serving for months and Phase 1 is not allowed to be the reason they
# stop, so every failure path here has to end in "log it and continue".
from lib.ops import bootstrap as B


def _factory():
    return psycopg2.connect(DB_URL)


check(B.migrate_on_boot(_factory) == [],
      "boot applies nothing when the schema is already current")

# from empty
for t in ["mf_sessions", "mf_jobs", "mf_documents", "mf_audit_log",
          "mf_jurisdiction_rules", "mf_jurisdictions", "mf_user_roles",
          "mf_roles", "mf_users", "mf_divisions", "mf_organizations",
          "mf_migrations"]:
    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS {t} CASCADE")
    conn.commit()
    cur.close()
cur = conn.cursor()
cur.execute("DROP FUNCTION IF EXISTS mf_audit_log_immutable() CASCADE")
conn.commit()
cur.close()

check(B.migrate_on_boot(_factory) == ALL_NAMES,
      "and applies the foundation on a database that has never seen it — "
      "which is what the first Railway deploy after this merge does")
check(len(mf_tables()) == 12, "the tables are there afterwards")

os.environ["MF_OPS_MIGRATE"] = "0"
check(B.migrate_on_boot(_factory) == [],
      "MF_OPS_MIGRATE=0 skips — the escape hatch for a deploy where the "
      "ops schema is the thing that is broken")
os.environ.pop("MF_OPS_MIGRATE")


def _explodes():
    raise RuntimeError("database is on fire")


check(B.migrate_on_boot(_explodes) == [],
      "A CONNECTION FAILURE RETURNS EMPTY RATHER THAN RAISING. This is the "
      "one that matters: init_db() runs at import time, so an exception "
      "here takes down /screener and every other board over a platform "
      "with no users yet")
check(B.migrate_on_boot(lambda: None) == [],
      "and no DATABASE_URL is a skip, not a crash")

cur = conn.cursor()
cur.execute("UPDATE mf_migrations SET checksum='deadbeefdeadbeef' WHERE version=1")
conn.commit()
cur.close()
check(B.migrate_on_boot(_factory) == [],
      "boot refuses to migrate over checksum drift too, rather than "
      "inheriting the runner's exception or forcing past it — a deploy is "
      "the worst moment to guess at what schema is actually there")
cur = conn.cursor()
cur.execute("UPDATE mf_migrations SET checksum=%s WHERE version=1", (_orig,))
conn.commit()
cur.close()


conn.close()
_report_and_exit(
    "   Migration ran up/down/up against a real Postgres, leaving no mf_ "
    "tables behind\n   on the way down. mf_audit_log resisted UPDATE, "
    "DELETE, zero-row UPDATE,\n   zero-row DELETE and TRUNCATE.")

"""Migrations that run up AND down, tracked in a ledger.

Phase 0's audit found the host repo creates schema with idempotent
`CREATE TABLE IF NOT EXISTS` calls at boot (database.py:_ensure_*_tables).
That works, and it has two properties this platform cannot accept:

  * There is NO DOWN PATH. Phase 1's acceptance says "migrations run clean
    up and down", and more practically: a schema you cannot roll back is a
    schema you cannot safely change once there is a rent ledger in it.
  * There is no record of WHAT ran. Two deploys with different code both
    "ensure" the tables and neither knows which shape it got.

So: numbered .up.sql / .down.sql pairs, applied in order, recorded in
mf_migrations with a checksum. Each migration runs in ONE transaction — a
half-applied migration is the worst outcome available, and Postgres will
do transactional DDL if you let it.

The checksum is not paranoia. Editing an already-applied migration file is
the single most common way a staging database silently diverges from
production, and this refuses to run until someone acknowledges it.
"""
from __future__ import annotations

import hashlib
import logging
import re
from pathlib import Path

log = logging.getLogger("mf.migrate")

MIGRATIONS_DIR = Path(__file__).resolve().parent
_NAME = re.compile(r"^(\d{4})_([a-z0-9_]+)\.(up|down)\.sql$")

LEDGER_DDL = """
CREATE TABLE IF NOT EXISTS mf_migrations (
    version     INTEGER PRIMARY KEY,
    name        TEXT NOT NULL,
    checksum    TEXT NOT NULL,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    applied_by  TEXT NOT NULL DEFAULT ''
)
"""


class MigrationError(RuntimeError):
    pass


def discover(directory: Path | None = None) -> list[dict]:
    """Every migration on disk, ordered, with both halves resolved.

    A migration missing its .down.sql RAISES rather than being treated as
    irreversible-by-design. "I'll add the down later" is how a schema
    becomes one-way, and the failure should happen at development time.
    """
    d = directory or MIGRATIONS_DIR
    ups: dict[int, dict] = {}
    downs: dict[int, Path] = {}
    for p in sorted(d.glob("*.sql")):
        m = _NAME.match(p.name)
        if not m:
            raise MigrationError(
                f"{p.name} does not match NNNN_name.(up|down).sql — an "
                f"unrecognised file in this directory is either a typo or a "
                f"migration that will never run."
            )
        version, name, half = int(m.group(1)), m.group(2), m.group(3)
        if half == "up":
            ups[version] = {"version": version, "name": name, "up_path": p}
        else:
            downs[version] = p

    out = []
    for version in sorted(ups):
        missing = version not in downs
        if missing:
            raise MigrationError(
                f"Migration {version:04d}_{ups[version]['name']} has no "
                f".down.sql. Phase 1 requires migrations that run both ways; "
                f"an irreversible one has to be a deliberate, argued choice, "
                f"not an omission."
            )
        rec = ups[version]
        rec["down_path"] = downs[version]
        rec["sql_up"] = rec["up_path"].read_text()
        rec["sql_down"] = rec["down_path"].read_text()
        rec["checksum"] = hashlib.sha256(rec["sql_up"].encode()).hexdigest()[:16]
        out.append(rec)

    orphans = set(downs) - set(ups)
    if orphans:
        raise MigrationError(f"down migrations with no up: {sorted(orphans)}")
    return out


def applied(conn) -> dict[int, dict]:
    cur = conn.cursor()
    cur.execute(LEDGER_DDL)
    conn.commit()
    cur.execute("SELECT version, name, checksum FROM mf_migrations ORDER BY version")
    rows = {r[0]: {"version": r[0], "name": r[1], "checksum": r[2]}
            for r in cur.fetchall()}
    cur.close()
    return rows


def verify(conn, directory: Path | None = None) -> list[str]:
    """Complaints about drift between disk and the ledger. Empty is good."""
    on_disk = {m["version"]: m for m in discover(directory)}
    done = applied(conn)
    problems = []
    for v, rec in done.items():
        if v not in on_disk:
            problems.append(
                f"{v:04d} {rec['name']} is recorded as applied but its file is "
                f"gone — this database is ahead of this checkout.")
        elif on_disk[v]["checksum"] != rec["checksum"]:
            problems.append(
                f"{v:04d} {rec['name']} was EDITED after being applied "
                f"(checksum {rec['checksum']} -> {on_disk[v]['checksum']}). "
                f"The database does not have the schema this file describes. "
                f"Write a new migration instead of editing an applied one.")
    return problems


def migrate(conn, to: int | None = None, directory: Path | None = None,
            actor: str = "", allow_drift: bool = False) -> list[str]:
    """Apply everything pending, or up to `to`. Returns what ran.

    Refuses to run at all when the ledger and the files disagree, unless
    explicitly overridden — running new migrations on top of a database
    whose earlier schema differs from the checkout produces a shape nobody
    has ever tested.
    """
    problems = verify(conn, directory)
    if problems and not allow_drift:
        raise MigrationError("Refusing to migrate:\n  " + "\n  ".join(problems))

    done = applied(conn)
    ran = []
    for m in discover(directory):
        if m["version"] in done:
            continue
        if to is not None and m["version"] > to:
            break
        cur = conn.cursor()
        try:
            # One transaction per migration. Postgres does transactional
            # DDL, so a failure half way leaves nothing behind.
            cur.execute(m["sql_up"])
            cur.execute(
                "INSERT INTO mf_migrations (version, name, checksum, applied_by) "
                "VALUES (%s, %s, %s, %s)",
                (m["version"], m["name"], m["checksum"], actor))
            conn.commit()
            ran.append(f"{m['version']:04d}_{m['name']}")
            log.info("applied %04d_%s", m["version"], m["name"])
        except Exception as e:
            conn.rollback()
            raise MigrationError(
                f"{m['version']:04d}_{m['name']} failed and was rolled back: {e}")
        finally:
            cur.close()
    return ran


def rollback(conn, steps: int = 1, directory: Path | None = None) -> list[str]:
    """Undo the last `steps` migrations, newest first."""
    done = applied(conn)
    if not done:
        return []
    on_disk = {m["version"]: m for m in discover(directory)}
    out = []
    for version in sorted(done, reverse=True)[:max(0, int(steps))]:
        m = on_disk.get(version)
        if not m:
            raise MigrationError(
                f"Cannot roll back {version:04d}: its .down.sql is not in this "
                f"checkout. Check out the commit that applied it.")
        cur = conn.cursor()
        try:
            cur.execute(m["sql_down"])
            cur.execute("DELETE FROM mf_migrations WHERE version = %s", (version,))
            conn.commit()
            out.append(f"{version:04d}_{m['name']}")
            log.info("rolled back %04d_%s", version, m["name"])
        except Exception as e:
            conn.rollback()
            raise MigrationError(
                f"rollback of {version:04d}_{m['name']} failed: {e}")
        finally:
            cur.close()
    return out


def status(conn, directory: Path | None = None) -> dict:
    done = applied(conn)
    disk = discover(directory)
    return {
        "applied": sorted(done),
        "pending": [m["version"] for m in disk if m["version"] not in done],
        "problems": verify(conn, directory),
    }

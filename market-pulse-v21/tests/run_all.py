"""Run every test suite — the one command CI and a developer both use.

    python tests/run_all.py
    TEST_PG=postgresql://postgres:postgres@localhost:5432 python tests/run_all.py

Each tests/test_*.py is a script that exits 0 when every check passes. This
runs them all, one process each, prints a line per suite, shows the tail of
any that fail, and exits non-zero if one did.

POSTGRES. Eight suites have a second half that needs a real database and
skips without one. With TEST_PG set (a server URL, no database name), each
suite that reads DATABASE_URL gets a FRESH database of its own — they apply
migrations and some run them down and up again, so they must not share. A
suite that still reports "no DATABASE_URL" when a database was provided
FAILS here: a skipped half that looks like a pass is how a broken
connection would go unnoticed. Suites that don't use a database never see
DATABASE_URL, so the page tests don't start the app against it.
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TESTS = sorted(p for p in (ROOT / "tests").glob("test_*.py"))
SKIP_MARK = "no DATABASE_URL"
MIN_SUITES = 40              # a glob that finds far fewer is a broken checkout, not a pass


def with_db(base: str, name: str) -> str:
    """The server URL with its database set to `name` — the path only, so a
    socket URL (postgresql://u@/?host=/var/tmp&port=5433) keeps its query."""
    from urllib.parse import urlsplit, urlunsplit
    u = urlsplit(base)
    return urlunsplit((u.scheme, u.netloc, f"/{name}", u.query, u.fragment))


def fresh_db(base: str, name: str) -> str:
    import psycopg2
    conn = psycopg2.connect(with_db(base, "postgres"))
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(f'DROP DATABASE IF EXISTS "{name}"')
            cur.execute(f'CREATE DATABASE "{name}"')
    finally:
        conn.close()
    return with_db(base, name)


def main() -> int:
    base = os.environ.get("TEST_PG", "")
    if len(TESTS) < MIN_SUITES:
        print(f"only {len(TESTS)} suites found (expected {MIN_SUITES}+) — refusing to report a pass")
        return 1
    failed = []
    t_all = time.time()
    for path in TESTS:
        env = {k: v for k, v in os.environ.items() if k != "DATABASE_URL"}
        uses_db = "DATABASE_URL" in path.read_text()
        if base and uses_db:
            env["DATABASE_URL"] = fresh_db(base, f"t_{path.stem}")
        t = time.time()
        try:
            r = subprocess.run([sys.executable, str(path)], cwd=ROOT, env=env,
                               capture_output=True, text=True, timeout=900)
            out, rc = r.stdout + r.stderr, r.returncode
        except subprocess.TimeoutExpired as e:
            out, rc = f"{e.stdout or ''}{e.stderr or ''}\nTIMED OUT after 900s", 1
        if rc == 0 and base and uses_db and SKIP_MARK in out:
            rc = 1
            out += (f"\nFAILED BY run_all: a database was provided (TEST_PG) but the "
                    f"suite still reported '{SKIP_MARK}'.")
        last = next((ln for ln in reversed(out.strip().splitlines()) if ln.startswith(("OK", "FAIL", "SKIP"))),
                    out.strip().splitlines()[-1] if out.strip() else "")
        mark = "ok  " if rc == 0 else "FAIL"
        db = " [db]" if base and uses_db else ""
        print(f"{mark} {time.time() - t:6.1f}s  {path.name}{db}  {last[:110]}", flush=True)
        if rc != 0:
            failed.append(path.name)
            print("\n".join("      " + ln for ln in out.strip().splitlines()[-40:]), flush=True)
    print(f"\n{len(TESTS) - len(failed)}/{len(TESTS)} suites passed in {time.time() - t_all:.0f}s"
          + ("" if base else " (no TEST_PG: database halves skipped)"))
    if failed:
        print("failed: " + ", ".join(failed))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

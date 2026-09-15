"""Bringing the ops schema up at boot, without being able to take the app down.

The analysis boards have been running for months. Nothing in Phase 1 is
allowed to change that, so this module has exactly one rule: an ops
migration failure is logged loudly and swallowed. A half-built platform
that nobody is using yet must never be the reason /screener returns 502.

That is the opposite of the usual advice — normally a failed migration
SHOULD stop a deploy, because serving traffic against a schema you did
not expect is how data gets corrupted. It does not apply here yet: there
are no ops routes, no ops writes, and no ops data. When Phase 1-D puts
real traffic on these tables, this becomes a fail-closed check instead,
and BACKLOG.md carries that as an explicit item rather than a hope.

Set MF_OPS_MIGRATE=0 to skip entirely.
"""
from __future__ import annotations

import logging
import os

log = logging.getLogger("mf.bootstrap")


def migrate_on_boot(get_conn) -> list[str]:
    """Apply pending mf_ migrations. Returns what ran; never raises.

    `get_conn` is passed in rather than imported so this module has no
    dependency on the host app's database.py — the seam in
    ARCHITECTURE.md §2 points one way, and ops importing the analysis
    side would be the first crack in it.
    """
    if os.getenv("MF_OPS_MIGRATE", "1") == "0":
        log.info("ops migrations skipped (MF_OPS_MIGRATE=0)")
        return []

    conn = None
    try:
        from lib.ops.migrations import runner

        conn = get_conn()
        if not conn:
            log.warning("ops migrations skipped: no database connection")
            return []

        problems = runner.verify(conn)
        if problems:
            # Do not migrate over drift, and do not hide it either. This
            # is the case where the database has a schema no checkout
            # describes, and guessing is worse than waiting.
            for p in problems:
                log.error("ops migration drift: %s", p)
            return []

        ran = runner.migrate(conn, actor=f"boot:{os.getenv('RAILWAY_SERVICE_NAME', 'local')}")
        if ran:
            log.info("ops migrations applied: %s", ", ".join(ran))
        return ran
    except Exception as e:
        # Deliberately broad. See the module docstring: the ops platform
        # is not worth an outage on the boards that are actually in use.
        log.error("ops migrations FAILED (app continues without them): %s", e)
        return []
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

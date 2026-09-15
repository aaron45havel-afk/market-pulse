"""Bringing the ops schema up at boot, and refusing to serve ops without it.

TWO DIFFERENT THINGS FAIL IN TWO DIFFERENT DIRECTIONS HERE, and conflating
them is the mistake this module used to make.

  * THE PROCESS fails OPEN. init_db() runs at import time, so an exception
    raised here does not degrade the ops platform — it stops /screener,
    /norcal and every other board that has been serving for months. So
    migrate_on_boot() still never raises, and that is load-bearing.

  * THE OPS ROUTES fail CLOSED. Serving traffic against a schema you did
    not expect is how data gets corrupted, and Phase 1-D put real traffic
    on the mf_ tables. So every exit path below records whether the schema
    is known-good, and routers/ops/deps.db() refuses the request when it
    is not.

The old rule — log it, swallow it, and let ops serve anyway — was correct
exactly while there were no ops routes, no ops writes and no ops data. It
stopped being correct the moment the portals shipped.

DEFAULT IS NOT READY. If migrate_on_boot() is never called at all — the
import fails, database.py changes shape, somebody mounts the routers in a
new process — ops is closed rather than open. A readiness flag that
defaults to true only works when you remember to set it.

MF_OPS_MIGRATE=0 skips APPLYING migrations. It does not skip CHECKING
them: the schema is still verified, and ops serves only if it is already
current. That keeps the escape hatch meaning "do not write to my schema"
rather than quietly also meaning "and do not look at it either", which is
the reading that would put the platform back on an unknown database.

There is deliberately no flag to force the gate open. An override on a
safety gate is a thing that gets used at 3am; if ops has to come up, the
fix is to make the schema current.
"""
from __future__ import annotations

import logging
import os

log = logging.getLogger("mf.bootstrap")

_NEVER_RAN = "ops migrations have not run in this process"

# Not ready until something proves otherwise.
_READY = False
_REASON = _NEVER_RAN


def _mark(ready: bool, reason: str) -> None:
    global _READY, _REASON
    _READY, _REASON = ready, reason


def schema_ready() -> bool:
    """Is the mf_ schema known to match this checkout?"""
    return _READY


def schema_state() -> dict:
    """{ready, reason} — the reason is what a 503 and a log line quote."""
    return {"ready": _READY, "reason": _REASON}


def reset_state() -> None:
    """Back to not-ready. For tests, and for a process re-running boot."""
    _mark(False, _NEVER_RAN)


def migrate_on_boot(get_conn) -> list[str]:
    """Apply pending mf_ migrations. Returns what ran; NEVER raises.

    `get_conn` is passed in rather than imported so this module has no
    dependency on the host app's database.py — the seam in
    ARCHITECTURE.md §2 points one way, and ops importing the analysis
    side would be the first crack in it.

    The return value is what ran. Whether the schema is USABLE is a
    separate question, answered by schema_ready(), because "nothing ran"
    is the correct answer both when the schema is already current and
    when the database is on fire.
    """
    skip_apply = os.getenv("MF_OPS_MIGRATE", "1") == "0"
    conn = None
    try:
        from lib.ops.migrations import runner

        conn = get_conn()
        if not conn:
            _mark(False, "no database connection at boot")
            log.warning("ops migrations skipped: no database connection")
            return []

        problems = runner.verify(conn)
        if problems:
            # Do not migrate over drift, and do not hide it either. This
            # is the case where the database has a schema no checkout
            # describes, and guessing is worse than waiting.
            for p in problems:
                log.error("ops migration drift: %s", p)
            _mark(False, f"schema drift: {problems[0]}")
            return []

        ran = []
        if skip_apply:
            log.info("ops migrations not applied (MF_OPS_MIGRATE=0); "
                     "verifying only")
        else:
            ran = runner.migrate(
                conn,
                actor=f"boot:{os.getenv('RAILWAY_SERVICE_NAME', 'local')}")
            if ran:
                log.info("ops migrations applied: %s", ", ".join(ran))

        # PENDING IS CHECKED AFTER APPLYING, not instead of it. Under
        # MF_OPS_MIGRATE=0 this is the whole gate; otherwise it catches a
        # migrate() that returned without erroring and without finishing.
        pending = runner.status(conn)["pending"]
        if pending:
            versions = ", ".join(f"{v:04d}" for v in pending)
            _mark(False, f"migrations not applied: {versions}")
            log.error("ops schema is behind this checkout: %s pending",
                      versions)
            return ran

        _mark(True, "schema current")
        return ran
    except Exception as e:
        # Deliberately broad, and the swallow is the point: see the module
        # docstring. The ops platform is not worth an outage on the boards
        # that are actually in use — but it does not get to serve either.
        _mark(False, f"migration check failed: {e}")
        log.error("ops migrations FAILED (boards continue, ops routes "
                  "refuse): %s", e)
        return []
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass

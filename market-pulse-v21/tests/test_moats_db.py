"""Permit-moat persistence — round-trips against a real Postgres.

Run:  python tests/test_moats_db.py

SKIPS (exit 0) when DATABASE_URL is unset, which is the normal case in
CI and in the sandbox. To run it for real:

    initdb -D /var/tmp/pg -A trust -U postgres
    pg_ctl -D /var/tmp/pg -o '-p 55432 -k /var/tmp' start
    createdb -h /var/tmp -p 55432 -U postgres moattest
    DATABASE_URL="postgresql://postgres@/moattest?host=/var/tmp&port=55432" \
        python tests/test_moats_db.py

The domain suite proves the rules. This proves the STORAGE keeps them,
which is a different question and the one where the damage is silent:
every failure mode below writes successfully, returns no error, and
destroys something the user cannot get back.

It uses its own tables and drops them at the end, so it is safe to point
at a scratch database — never at the production one.
"""
import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

if not os.environ.get("DATABASE_URL"):
    print("SKIP — no DATABASE_URL. See this file's docstring to run it.")
    sys.exit(0)

import database as D
import moats as M

_COUNT = 0
_FAILS = []


def check(cond, msg):
    global _COUNT
    _COUNT += 1
    if not cond:
        _FAILS.append(msg)


def _wipe():
    conn = D._get_conn()
    cur = conn.cursor()
    for t in ("pm_rubrics", "pm_positions", "pm_holdings"):
        cur.execute(f"DROP TABLE IF EXISTS {t} CASCADE")
    conn.commit(); cur.close(); conn.close()


_wipe()
D._ensure_moat_tables()
NOW = datetime(2026, 8, 24, 12, 0)

# ── the seed ──
check(D.moat_seed_if_empty() == 12, "the twelve seed into an empty watchlist")
check(D.moat_seed_if_empty() == 0,
      "and seeding is idempotent — guarded on emptiness, so a seed the "
      "user deliberately archived is not silently reinstated on the next "
      "deploy")

rows = {r["ticker"]: r for r in D.moat_all()}
check(len(rows) == 12 and all(r["stage"] == "QUALIFIED" for r in rows.values()),
      "all twelve land at QUALIFIED")
check(rows["CMP"]["thesis"].startswith("Nobody is building another Goderich")
      and rows["CMP"]["anchorPrice"] == 28.50
      and str(rows["CMP"]["anchorAsOf"]) == "2026-08-01",
      "the prose and the anchor survive the round trip verbatim")
check(rows["CMP"]["rubric"]["grandfathered"] is True
      and rows["CMP"]["rubric"]["score"] is None,
      "with a grandfathered rubric whose score is NULL, not 0 — the column "
      "is nullable precisely so the seeds need not invent an assessment")
check(rows["MCEM"]["anchorPrice"] is None,
      "and an unknown anchor stays NULL rather than becoming zero")

# ── a price write must not touch anything else ──
cid = rows["CMP"]["id"]
D.moat_position_upsert(cid, {"targetPrice": 20.0, "lastPrice": 28.5,
                             "lastPriceAt": NOW})
D.moat_position_upsert(cid, M.lock_plan({}, "Buy a third at 20.", NOW))
D.moat_update(cid, {"stage": "ARMED"})

p = {r["ticker"]: r for r in D.moat_all()}["CMP"]["position"]
res = M.apply_price(p, 19.5, NOW + timedelta(days=3))
D.moat_position_upsert(cid, res["changed"])
p2 = {r["ticker"]: r for r in D.moat_all()}["CMP"]["position"]

check(p2["planLockedAt"] == NOW and p2["plan"] == "Buy a third at 20.",
      "TYPING A PRICE DOES NOT TOUCH THE PLAN OR ITS LOCK DATE. The upsert "
      "patches only the named columns; a blind whole-row upsert would "
      "silently erase the commitment date every time a price was entered, "
      "and nothing would look wrong until the day it mattered")
check(p2["triggeredAt"] is not None and p2["acknowledged"] is False,
      "and the trigger is stamped and unacknowledged")

D.moat_position_upsert(cid, {"acknowledged": True})
p3 = {r["ticker"]: r for r in D.moat_all()}["CMP"]["position"]
D.moat_position_upsert(cid, M.apply_price(p3, 26.0, NOW + timedelta(days=9))["changed"])
p4 = {r["ticker"]: r for r in D.moat_all()}["CMP"]["position"]
check(p4["triggeredAt"] == p2["triggeredAt"] and p4["acknowledged"] is True,
      "a recovery back above target leaves the stamp and the "
      "acknowledgement alone — the moment happened and the record of it "
      "outlives the price")

ed = M.edit_plan(p4, "Wait for 16 instead.", NOW + timedelta(days=10), confirmed=True)
D.moat_position_upsert(cid, ed)
p5 = {r["ticker"]: r for r in D.moat_all()}["CMP"]["position"]
check("Buy a third at 20." in p5["notes"] and "24 Aug 2026" in p5["notes"],
      "and a rewritten plan leaves the previous commitment, with the date "
      "it was made, in the notes")

# ── rubric versions ──
pesi = rows["PESI"]["id"]
GOOD = {"replicableWithCapital": False, "permitTrend": "DECLINING",
        "freightPctOfValue": 45, "terminalDemand50yr": "INTACT",
        "cheapBecause": "OPERATIONAL_STUMBLE",
        "evidence": "Checked the NRC licence register and the 2025 10-K; the "
                    "mixed-waste permit has not issued to a new operator "
                    "since 2011 and the DOE backlog is in the footnotes."}
check(D.moat_rubric_add(pesi, M.validate_rubric(GOOD)) == 2,
      "a re-review appends version 2")
hist = D.moat_rubric_history(pesi)
check(len(hist) == 2 and hist[1]["version"] == 1 and hist[1]["grandfathered"] is True,
      "AND THE ORIGINAL SURVIVES. Reading what you thought then beside "
      "what you think now is the only reason to keep these at all")
check({r["ticker"]: r for r in D.moat_all()}["PESI"]["rubric"]["version"] == 2,
      "while the board shows the newest")

D.moat_rubric_add(pesi, M.validate_rubric({**GOOD, "replicableWithCapital": True}))
after = {r["ticker"]: r for r in D.moat_all()}["PESI"]
check(after["rubric"]["passed"] is False and after["stage"] == "QUALIFIED",
      "a FAILING re-review is recorded but never auto-demotes — a person "
      "makes that call, and a stage that changed itself overnight would be "
      "a decision nobody made")
check(after["rubric_count"] == 3, "and every version is still counted")

# ── adding, and not overwriting ──
new_id = D.moat_add(M.validate_candidate(
    {"ticker": "vmc", "name": "Vulcan", "sourceNote": "aggregates"}))
check(new_id is not None, "a candidate inserts and returns its id")
check(D.moat_add(M.validate_candidate(
    {"ticker": "VMC", "name": "Overwrite me", "sourceNote": "no"})) is None,
      "a second add of the same ticker returns None rather than upserting")
check({r["ticker"]: r for r in D.moat_all()}["VMC"]["name"] == "Vulcan",
      "and the existing row is untouched — re-adding a ticker must never "
      "overwrite a thesis someone spent an evening writing")

D.moat_update(new_id, {"stage": "ARCHIVED", "archiveReason": "Too big."})
check(D.moat_count() == 13
      and {r["ticker"]: r for r in D.moat_all()}["VMC"]["archiveReason"] == "Too big.",
      "archiving sets a stage and a reason and keeps the row — nothing "
      "here is ever hard-deleted")

check(D.moat_update(new_id, {"ticker": "HACK", "id": 999}) is True
      and {r["id"]: r for r in D.moat_all()}[new_id]["ticker"] == "VMC",
      "identity columns are not updatable through the generic patch path, "
      "so a stray field in a request body cannot rewrite which company a "
      "row is about")


# ── the two properties an in-place UPDATE would silently break ──
#
# Both of these pass a naive test while destroying the thing they are
# for, so they are checked at the ROW level: the original rubric's row id
# and every column of it, and the archived holding's continued existence.
def _rubric_rows(hid):
    c = D._get_conn(); cur = c.cursor()
    cur.execute("SELECT id, version, grandfathered, score, evidence FROM pm_rubrics "
                "WHERE holding_id=%s ORDER BY version", (hid,))
    out = cur.fetchall(); cur.close(); c.close()
    return out


_gw = {r["ticker"]: r for r in D.moat_all()}["GWRS"]["id"]
_v1 = _rubric_rows(_gw)
D.moat_rubric_add(_gw, M.validate_rubric(GOOD))
_v2 = _rubric_rows(_gw)
check(len(_v2) == len(_v1) + 1, "a re-review INSERTs rather than updating")
check(_v1[0] == [r for r in _v2 if r[1] == 1][0],
      "and version 1's ROW IS BYTE-IDENTICAL afterwards — same id, same "
      "score, same evidence. An UPDATE in place would leave one row that "
      "still reads as 'a rubric exists' while the original assessment is "
      "gone, and no count-based check would notice")

_arch_before = D.moat_count()
D.moat_update(new_id, {"stage": "ARCHIVED",
                       "archiveReason": "Cremation mix worsened two years running",
                       "archivedAt": datetime(2026, 8, 24, 9, 0)})
_conn = D._get_conn(); _cur = _conn.cursor()
_cur.execute("SELECT stage, archived_at FROM pm_holdings WHERE id=%s", (new_id,))
_row = _cur.fetchone()
_cur.execute("SELECT ticker FROM pm_holdings WHERE archive_reason ILIKE %s",
             ("%cremation mix%",))
_found = [r[0] for r in _cur.fetchall()]
_cur.close(); _conn.close()
check(_row is not None and _row[0] == "ARCHIVED" and _row[1] is not None,
      "archiving sets the stage AND stamps archived_at")
check(D.moat_count() == _arch_before,
      "and DELETES nothing — the row count is unchanged")
check(_found == ["VMC"],
      "the archived row stays searchable by its reason, which is the whole "
      "reason a reason is required")

_wipe()

# ── report ──
if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for m in _FAILS:
        print("  ✗", m)
    sys.exit(1)
print(f"OK — all {_COUNT} permit-moat persistence checks passed.")
sys.exit(0)

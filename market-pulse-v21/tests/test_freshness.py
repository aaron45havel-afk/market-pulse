"""Freshness — the guard for builds that join instead of fetching.

Run:  python tests/test_freshness.py      (exit 0 = all pass)

Pure. The guard is a comparison between two small dicts, so every outcome
it can have is reachable without a file, a clock or a network.

THE CHECKS ARE WEIGHTED TOWARDS THE SKIP, not the fault. A build that
refuses to run when it should is visible within the hour — the board does
not update and somebody says so. A build that runs when it has nothing to
say is invisible forever: it writes the same numbers under a newer date,
both jobs go green, and the page looks refreshed. That is the failure this
module exists for, and the reason the obvious rule does not catch it.
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import freshness as F

_COUNT = 0
_FAILS = []


def check(cond, msg):
    global _COUNT
    _COUNT += 1
    if not cond:
        _FAILS.append(msg)


# ══════════════════════════════════════════════════════════════════
# PARSING — the formats that actually appear in this repo
# ══════════════════════════════════════════════════════════════════
check(F.stamp("2026-09-15") is not None
      and F.stamp("2026-09-07T13:45:58+00:00") is not None,
      "both real stamp formats parse — compounders and zip_neighborhoods "
      "write a bare date, schloss a full timestamp")
check(F.stamp("2026-09-15") > F.stamp("2026-09-07T13:45:58+00:00"),
      "A BARE DATE AND A FULL TIMESTAMP COMPARE AT ALL. "
      "datetime.fromisoformat returns a naive value for one and an aware "
      "value for the other, and comparing those raises TypeError — so "
      "without normalising to UTC the guard takes the build DOWN on the "
      "first month two such files are compared, rather than answering "
      "wrongly")
check(F.stamp("") is None and F.stamp(None) is None
      and F.stamp("last tuesday") is None and F.stamp(20260915) is None,
      "an absent or unparseable stamp is None, not a guess")

# A NAIVE STAMP MEANS UTC, NOT THE RUNNER'S CLOCK. This needs TZ set to
# test at all: on a UTC runner — which is every GitHub runner — reading a
# bare date as local time and reading it as UTC give the same answer, so
# the bug is invisible until the day something runs somewhere else.
_TZ_WAS = os.environ.get("TZ")
try:
    os.environ["TZ"] = "America/Los_Angeles"
    time.tzset()
    check(F.stamp("2026-09-15").isoformat() == "2026-09-15T00:00:00+00:00",
          "a bare date is midnight UTC even when the runner is not on UTC. "
          "datetime.astimezone() on a naive value assumes LOCAL time, which "
          "would shift one file's stamp by the runner's offset while "
          "leaving an aware stamp alone — a seven-hour disagreement "
          "between two files that are supposed to be compared")
finally:
    if _TZ_WAS is None:
        os.environ.pop("TZ", None)
    else:
        os.environ["TZ"] = _TZ_WAS
    time.tzset()


# ══════════════════════════════════════════════════════════════════
# WHICH KEYS COUNT AS INPUTS
# ══════════════════════════════════════════════════════════════════
check(F.input_keys({"a": 1, "logic": "x", "_file": "y"}) == ["a"],
      "the logic hash and caller bookkeeping are not inputs")
check(F.input_keys({"a": 1}, {"b": 2}) == ["a", "b"],
      "THE UNION, NOT THE INTERSECTION. An input in one dict and not the "
      "other has to surface as unverifiable, which is what blocks a skip. "
      "Intersecting would mean adding a new input silently NARROWED the "
      "guard — the opposite of what adding an input should do")
check(F.input_keys(None, None) == [] and F.input_keys() == [],
      "no dicts, no keys, no crash")


_A, _B = "aaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbb"


def _v(cur, prev, cur_l=_A, prev_l=_A):
    """verdict() over two {name: stamp} dicts, with logic hashes attached."""
    c = dict(cur, logic=cur_l) if cur_l is not None else dict(cur)
    p = dict(prev, logic=prev_l) if prev_l is not None else dict(prev)
    return F.verdict(c, p)


# ══════════════════════════════════════════════════════════════════
# THE DECISION TABLE
# ══════════════════════════════════════════════════════════════════
check(F.verdict({"a": "2026-09-15"}, None)["action"] == "build",
      "no previous snapshot means build — a first run has nothing to be "
      "stale against")
check(F.verdict({"logic": _A}, {"logic": _A})["action"] == "build",
      "AND SO DOES A BUILD THAT DECLARES NO INPUTS. Unchanged code over "
      "nothing declared is not evidence that nothing moved; it is evidence "
      "that the caller forgot to say what it reads")

check(_v({"x": "2026-09-15", "y": "2026-09-07"},
         {"x": "2026-08-12", "y": "2026-09-07"})["action"] == "build",
      "a forward input builds")
check(_v({"x": "2026-08-12", "y": "2026-09-07"},
         {"x": "2026-09-15", "y": "2026-09-07"})["action"] == "fault",
      "an input that went BACKWARD is a fault: the snapshot on disk was "
      "built from newer data than this checkout can see")
check(_v({"x": "2026-08-12", "y": "2026-10-01"},
         {"x": "2026-09-15", "y": "2026-09-07"})["action"] == "fault",
      "AND A BACKWARD INPUT OUTRANKS A FORWARD ONE. A run with one fresh "
      "input and one rolled back is not half right — it is an answer whose "
      "parts come from different months, which for a ratio is worse than "
      "either part being wrong alone")

# ── the bug this guard exists for ────────────────────────────────────
# On the FCF board's first real run the operator dispatched
# refresh-compounders (36 minutes) and then the build (13 seconds). It read
# the tree as it stood BEFORE compounders pushed, so its inputs were not
# older than the snapshot already on disk — they were identical to them,
# because that snapshot had been built from the very same unrefreshed files
# minutes earlier. Both jobs went green.
_HISTORICAL = _v({"compounders": "2026-08-12",
                  "schloss": "2026-09-07T13:45:58+00:00"},
                 {"compounders": "2026-08-12",
                  "schloss": "2026-09-07T13:45:58+00:00"})
check(_HISTORICAL["action"] == "skip",
      "THE HISTORICAL FAILURE IS CAUGHT: identical inputs and unchanged "
      "code produce a skip, not a board")
check(_HISTORICAL["action"] != "build",
      "MUTATION — had the rule been the obvious 'refuse if the inputs are "
      "OLDER than the snapshot on disk', this exact case would have passed "
      "it, because nothing was older. Backward movement is a different and "
      "rarer fault. The load-bearing half of this guard is the one that "
      "refuses when nothing moved AT ALL")
check("has NOT" in _HISTORICAL["reason"],
      "and the message says a refresh did not land, because that — not "
      "'no changes' — is what an operator who dispatched one needs to read")

check(_v({"x": "2026-08-12"}, {"x": "2026-08-12"}, cur_l=_B)["action"]
      == "build",
      "unchanged inputs but CHANGED CODE builds: that is the push "
      "trigger's whole case, and a skip there would leave the page showing "
      "an answer the current code would not produce")
_NO_PREV_LOGIC = _v({"x": "2026-08-12"}, {"x": "2026-08-12"}, prev_l=None)
check(_NO_PREV_LOGIC["action"] == "build"
      and "predates logic stamping" in _NO_PREV_LOGIC["reason"],
      "a snapshot written before logic stamping cannot prove the code is "
      "unchanged, so it builds — AND SAYS WHY. Falling through to the hash "
      "comparison would also build, by accident of None never equalling a "
      "hash, and would report 'the code changed' about a snapshot that "
      "simply predates the field")
check(F.verdict({"x": "2026-08-12"}, {"x": "2026-08-12"})["action"] == "build",
      "AND IF BOTH HASHES ARE MISSING IT STILL BUILDS. That is the case "
      "the explicit branch exists for: None == None would otherwise read "
      "as 'the code is unchanged' and skip, concluding from two absent "
      "values that nothing moved")

# ── unverifiable is not unchanged ────────────────────────────────────
_UNKNOWN = _v({"x": None, "y": "2026-09-07"},
              {"x": "2026-08-12", "y": "2026-09-07"})
check(_UNKNOWN["action"] == "build" and _UNKNOWN["verified"] is False,
      "A MISSING STAMP DOES NOT EARN A SKIP. It cannot prove nothing "
      "moved, so the build proceeds and records that freshness was not "
      "verified — refusing instead would let a source file that stops "
      "writing as_of silently stop the board forever")
check(_v({"x": "garbage", "y": "2026-09-07"},
         {"x": "2026-08-12", "y": "2026-09-07"})["action"] == "build",
      "a stamp that stops being a date on one side builds. It cannot be "
      "ordered, but it is plainly not equal, so the run is not redundant")
check(_v({"x": "2026-08-12"}, {"x": "2026-08-12"})["verified"] is True,
      "and a genuine skip IS verified, so the two cases are "
      "distinguishable in the snapshot rather than both reading as 'fine'")
_NEW_INPUT = _v({"x": "2026-08-12", "brand_new": "2026-09-01"},
                {"x": "2026-08-12"})
check(_NEW_INPUT["action"] == "build" and _NEW_INPUT["verified"] is False,
      "ADDING AN INPUT BUILDS. The new key is in no previous snapshot, so "
      "it cannot be compared, so the run cannot be called redundant — "
      "which is right, because a build that just grew an input is exactly "
      "the build whose output should change")

# ── every input is watched, not just the first ───────────────────────
check(_v({"x": "2026-08-12", "y": "2026-09-07"},
         {"x": "2026-08-12", "y": "2026-08-01"})["action"] == "build",
      "a second input moving forward builds on its own — the schloss half "
      "is the one the FCF chain was rewired for, and a guard that watched "
      "only the first key would have missed the stale-market-cap case")
check(len(_v({"a": "2026-01-01", "b": "2026-01-01", "c": "2026-01-01"},
             {"a": "2026-01-01", "b": "2026-01-01",
              "c": "2026-01-01"})["moved"]) == 3,
      "the verdict reports on every declared input, however many, so the "
      "snapshot records what was checked rather than that something was")


# ══════════════════════════════════════════════════════════════════
# CONTENT STAMPS — for inputs that carry no usable date
# ══════════════════════════════════════════════════════════════════
check(F.compare("2026-09-15", "2026-08-12") == "newer"
      and F.compare("2026-08-12", "2026-09-15") == "older"
      and F.compare("2026-09-15", "2026-09-15") == "same",
      "dates compare by ORDER, so a rollback is visible")
check(F.compare("sha256:aaaa", "sha256:aaaa") == "same"
      and F.compare("sha256:bbbb", "sha256:aaaa") == "changed",
      "content hashes compare by EQUALITY — the exact answer to 'did this "
      "move', which is the question the skip turns on")
check(F.compare("sha256:bbbb", "sha256:aaaa") != "older",
      "A HASH IS NEVER 'OLDER', and that is a real reduction in cover, not "
      "a detail. An input declared by hash cannot reach the fault branch, "
      "so a checkout predating a refresh of THAT input rebuilds quietly "
      "instead of stopping. Guessing 'older' instead would fail the build "
      "on every ordinary refresh, which is worse — but a date is stronger "
      "and is preferred wherever the producer writes one")
check(F.compare(None, "x") is None and F.compare("x", None) is None,
      "a stamp missing on either side concludes nothing")
check(_v({"a": "sha256:new", "b": "2026-09-07"},
         {"a": "sha256:old", "b": "2026-09-07"})["action"] == "build",
      "a hashed input and a dated one MIX in one build: each is only ever "
      "compared against itself, so the kinds never meet")
_HASH_SKIP = _v({"a": "sha256:same"}, {"a": "sha256:same"})
check(_HASH_SKIP["action"] == "skip" and _HASH_SKIP["verified"] is True,
      "and an unchanged hash earns a skip like an unchanged date — "
      "otherwise a build whose inputs are all hashed could never skip, "
      "which is a guard that always builds")

_TMP = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                    ".freshness_probe.tmp")
try:
    with open(_TMP, "wb") as fh:
        fh.write(b"one")
    _S1 = F.content_stamp(_TMP)
    with open(_TMP, "wb") as fh:
        fh.write(b"two")
    _S2 = F.content_stamp(_TMP)
    check(_S1 != _S2 and _S1.startswith("sha256:"),
          "content_stamp follows the bytes, so it works on a SQLite file "
          "as well as on JSON")
    check(F.stamp(_S1) is None,
          "AND IT DOES NOT ACCIDENTALLY PARSE AS A DATE. If it did, two "
          "hashes would be ordered against each other and one of them "
          "would read as a rollback")
finally:
    if os.path.exists(_TMP):
        os.unlink(_TMP)
check(F.content_stamp("/nonexistent/path/nothing.db") is None,
      "an unreadable input stamps as None, which reads as unverifiable "
      "and builds — it does not crash the run or silently match")


# ══════════════════════════════════════════════════════════════════
# THE OPERATOR'S TWO LINES
# ══════════════════════════════════════════════════════════════════
_LINES = F.describe({"compounders": "2026-09-15", "schloss": "2026-09-07",
                     "logic": _A},
                    {"compounders": "2026-08-12", "schloss": "2026-09-07",
                     "logic": _A, "_file": "2026-09.json"})
check(len(_LINES) == 2 and "2026-09-15" in _LINES[0]
      and "2026-08-12" in _LINES[1] and "2026-09.json" in _LINES[1],
      "describe() prints both runs' dates and which file the old ones came "
      "from — THE DATES ARE WHAT NAME THE REFRESH THAT FAILED TO LAND, and "
      "the verdict sentence only says that one did")
check("logic" not in _LINES[0],
      "and it does not print the hash, which tells an operator nothing")
check(len(F.describe({"x": "2026-09-15"}, None)) == 2,
      "a first run still prints its inputs rather than special-casing "
      "itself into silence")


# ══════════════════════════════════════════════════════════════════
# THE CODE STAMP
# ══════════════════════════════════════════════════════════════════
_HERE = os.path.dirname(os.path.abspath(__file__))
_SELF = os.path.abspath(__file__)
_OTHER = os.path.join(os.path.dirname(_HERE), "freshness.py")
check(F.logic_stamp([_SELF]) == F.logic_stamp([_SELF]),
      "the code stamp is stable across calls")
check(F.logic_stamp([_SELF]) != F.logic_stamp([_OTHER]),
      "and differs for different content")
check(F.logic_stamp([_SELF, _OTHER]) != F.logic_stamp([_OTHER, _SELF]),
      "ORDER IS PART OF THE STAMP, so a caller that reorders its paths "
      "gets a rebuild rather than a silent match. Cheap, and the "
      "alternative is a hash that two different call sites can collide on")


if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for m in _FAILS:
        print("  ✗", m)
    sys.exit(1)
print(f"OK — all {_COUNT} freshness checks passed.")
print("   The skip is load-bearing, unverifiable never reads as unchanged,\n"
      "   and a bare date is UTC even when the runner is not.")
sys.exit(0)

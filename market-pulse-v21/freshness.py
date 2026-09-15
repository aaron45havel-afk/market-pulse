"""Is there anything new to say? — the guard for builds that JOIN rather than fetch.

Most refresh scripts in this repo fetch: they go to SEC or Zillow or FRED,
and whatever comes back is by definition this run's data. A few do not.
They read files already committed to the repo and do arithmetic over them,
which makes them fast — thirteen seconds against the thirty-six minutes
their inputs take — and that speed is the whole problem. A join can finish
before the fetch it depends on has pushed, read the tree as it stood
BEFORE, and publish last month's numbers under this month's date. Silently.
With a green tick on both jobs. It happened to the FCF-quality board on its
first real run.

────────────────────────────────────────────────────────────────────
THE OBVIOUS RULE DOES NOT CATCH IT
────────────────────────────────────────────────────────────────────
"Refuse if the inputs are older than the snapshot on disk" sounds like the
rule, and it was the first thing tried. On that run the inputs were not
older — they were IDENTICAL, because the snapshot on disk had been built
from the very same unrefreshed files minutes earlier. Nothing was stale
relative to anything. Backward movement is a real fault but a different
and rarer one.

What separates a real rebuild from that run is whether ANYTHING changed:
an input, or the code that reads them. If neither moved, re-running cannot
produce a different answer — only a newer date on the old one, which is
this repo's house failure exactly. So:

    an input went BACKWARD          fault — something is wrong, fail loudly
    an input moved forward          build
    nothing moved, logic changed    build
    nothing moved, logic unchanged  skip — say so, write nothing, exit 0

THE SKIP MUST NOT BE RED. A fallback cron firing against inputs that have
not moved is a normal Tuesday. The only wrong outcome is writing the file.

────────────────────────────────────────────────────────────────────
TWO KINDS OF STAMP, AND WHAT THE SECOND COSTS
────────────────────────────────────────────────────────────────────
An input is declared by a DATE where its file carries one, and by a
CONTENT HASH where it does not — zips.db is 28MB of SQLite with no
metadata, norcal_condo.json writes a bare "2026-07" that fromisoformat
rejects. Each input is only ever compared against itself, so the two kinds
mix freely within one build.

They are not equally strong. A date can be seen to have gone BACKWARD; a
hash can only be seen to differ. So an input declared by hash never
reaches the fault branch, and a checkout that predates a refresh of THAT
input reads as an ordinary change and rebuilds rather than stopping.
Prefer a date wherever the producer offers one, and read a hash-only
input set as "this run is not redundant" rather than "this run is sound".

────────────────────────────────────────────────────────────────────
WHAT THIS IS NOT FOR
────────────────────────────────────────────────────────────────────
A script that FETCHES has no use for this: its inputs are the network, and
"did anything move" is answered by the fetch itself. Those scripts need a
different guard — a delta guard on the size of the result, which
refresh_lynch_screener and refresh_hundred already carry. Do not confuse
the two. This one asks "is this run redundant"; that one asks "is this run
broken".
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from pathlib import Path

# Keys in an input dict that are not inputs. "logic" is the code stamp and
# is compared separately; anything underscore-prefixed is caller bookkeeping
# (which file the previous stamps came from, and so on).
LOGIC_KEY = "logic"


def input_keys(*dicts) -> list:
    """The input names across one or more stamp dicts, sorted.

    The UNION, not the intersection. An input present in one and absent
    from the other cannot be compared, and a key that cannot be compared
    must not quietly vanish from the comparison — it has to surface as
    unverifiable, which is what blocks a skip. Taking the intersection
    would mean adding a new input silently narrowed the guard.
    """
    keys = set()
    for d in dicts:
        if not d:
            continue
        keys |= {k for k in d
                 if k != LOGIC_KEY and not str(k).startswith("_")}
    return sorted(keys)


def stamp(value):
    """Parse a source file's date stamp to an aware UTC datetime, or None.

    Source files do not agree on format and never have: schloss writes a
    full '2026-09-07T13:45:58+00:00', compounders a bare '2026-09-15'.
    datetime.fromisoformat returns an AWARE value for the first and a NAIVE
    one for the second, and comparing those raises TypeError rather than
    returning a wrong answer — so the failure is a dead build on the first
    month two such files are compared, not a bad number.

    A naive stamp is assigned UTC EXPLICITLY rather than handed to
    astimezone(), which would read it as the RUNNER's local time and shift
    it by the runner's offset while leaving an aware stamp alone. Both do
    the same thing on a UTC runner — which is every GitHub runner — so the
    difference only shows up somewhere else, and the tests set TZ to prove
    it is load-bearing.
    """
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        dt = datetime.fromisoformat(value.strip())
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def content_stamp(path) -> str | None:
    """A file's content hash, for an input that carries no usable date.

    Not every input announces when it was made. zips.db is 28MB of SQLite
    with no metadata at all; norcal_condo.json writes a bare "2026-07"
    that fromisoformat rejects. Declaring those by date would mean
    declaring them as unverifiable, and an unverifiable input blocks every
    skip — a guard that always builds is not a guard.

    A hash answers "did this move" EXACTLY, which is the question, and
    gives up only the ordering. So an input stamped this way can be seen
    to have changed but never to have gone BACKWARD; the fault branch
    simply does not apply to it. That is a real reduction in cover and is
    the reason a dated stamp is preferred wherever a file offers one.
    """
    try:
        return "sha256:" + hashlib.sha256(Path(path).read_bytes()).hexdigest()[:16]
    except OSError:
        return None


def compare(cur, prev) -> str | None:
    """How one input moved: newer / older / same / changed, or None.

    Dates compare by ORDER, so a rollback is visible as "older".
    Anything else that is a string on both sides compares by EQUALITY and
    reports "changed" — no ordering, so no rollback detection, but "did
    this move" is answered exactly. A stamp missing on either side is
    None, meaning nothing can be concluded.
    """
    if cur is None or prev is None:
        return None
    c, p = stamp(cur), stamp(prev)
    if c is not None and p is not None:
        return "newer" if c > p else "older" if c < p else "same"
    if isinstance(cur, str) and isinstance(prev, str):
        return "same" if cur == prev else "changed"
    return None


def logic_stamp(paths) -> str:
    """Content hash of the code that decides what a build says.

    Bytes, not a version number: a stamp that depends on somebody
    remembering to bump it stops being true. Order matters and is the
    caller's, so pass the paths in a fixed order.
    """
    h = hashlib.sha256()
    for p in paths:
        h.update(Path(p).read_bytes())
    return h.hexdigest()[:16]


def verdict(current: dict, previous: dict | None) -> dict:
    """Should this build write? -> {action, reason, moved, verified}

    `current` and `previous` map input names to their source files' own
    date stamps, plus a "logic" content hash. Input names are whatever the
    caller uses; only "logic" and underscore-prefixed keys are special.

    action is "build", "skip" or "fault". ONLY "fault" is an error.

    UNVERIFIABLE IS NOT UNCHANGED. A missing or unparseable stamp cannot
    prove that nothing moved, so it does not earn a skip: the build
    proceeds and reports verified=False for the caller to record. Refusing
    instead would mean a source file that quietly stops writing its stamp
    stops the board forever, trading a stale snapshot for no snapshot.
    """
    if not previous:
        return {"action": "build", "reason": "no previous snapshot to compare",
                "moved": {}, "verified": False}

    keys = input_keys(current, previous)
    if not keys:
        return {"action": "build", "moved": {}, "verified": False,
                "reason": "no inputs declared, so nothing can be compared"}

    moved = {key: compare(current.get(key), previous.get(key)) for key in keys}

    # Backward first: a fault outranks anything else that moved. A run
    # that reads one fresh input and one rolled-back one is not half
    # right — it is an answer whose parts come from different months.
    backward = [k for k in keys if moved[k] == "older"]
    if backward:
        detail = ", ".join(
            f"{k} {previous.get(k)} -> {current.get(k)}" for k in backward)
        return {
            "action": "fault",
            "reason": (f"input went BACKWARD ({detail}). The snapshot on "
                       f"disk was built from newer data than this run can "
                       f"see, which means this checkout predates a refresh "
                       f"that already landed. Writing would overwrite good "
                       f"numbers with old ones."),
            "moved": moved, "verified": True,
        }

    # "changed" sits here rather than beside "older" on purpose: a content
    # stamp cannot tell forward from backward, and guessing the worse of
    # the two would fail builds over ordinary refreshes.
    forward = [k for k in keys if moved[k] in ("newer", "changed")]
    if forward:
        return {"action": "build", "moved": moved, "verified": True,
                "reason": f"fresh input: {', '.join(forward)}"}

    unverifiable = [k for k in keys if moved[k] is None]
    if unverifiable:
        return {"action": "build", "moved": moved, "verified": False,
                "reason": (f"cannot verify freshness of "
                           f"{', '.join(unverifiable)} — no usable date "
                           f"stamp, so 'unchanged' cannot be proved")}

    cur_logic, prev_logic = current.get(LOGIC_KEY), previous.get(LOGIC_KEY)
    if not prev_logic:
        return {"action": "build", "moved": moved, "verified": True,
                "reason": ("previous snapshot predates logic stamping, so "
                           "an unchanged screen cannot be proved")}
    if cur_logic != prev_logic:
        return {"action": "build", "moved": moved, "verified": True,
                "reason": "inputs unchanged but the build's own code changed"}

    return {
        "action": "skip",
        "reason": ("nothing to rebuild: both the inputs and the build's own "
                   "code are unchanged since the last snapshot. If you "
                   "expected a refresh to have landed, it has NOT — check "
                   "the input dates against the job you dispatched."),
        "moved": moved, "verified": True,
    }


def describe(current: dict, previous: dict | None) -> list:
    """Two aligned lines of input dates, for a build script to print.

    The dates are what tell an operator WHICH refresh failed to land; the
    verdict sentence only says that one did. Printed on every outcome,
    including a successful build, so a log that is read later can be
    checked rather than trusted.
    """
    keys = input_keys(current, previous)
    cur = "  ".join(f"{k} {current.get(k)}" for k in keys)
    if not previous:
        return [f"inputs   {cur}", "on disk  (nothing)"]
    prev = "  ".join(f"{k} {previous.get(k)}" for k in keys)
    where = previous.get("_file")
    return [f"inputs   {cur}",
            f"on disk  {prev}" + (f"   ({where})" if where else "")]

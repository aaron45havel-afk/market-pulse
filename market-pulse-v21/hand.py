"""What a person reads, kept apart from what the machine measured.

Six of the thirteen criteria are yours to answer by opening a document.
This module is where those answers live, and its whole design is one
decision: **hand-entered values are not in `measures`.**

They could have been. A `source` field on `Measure` plus a check in the
counting loop would work, and it is what a design review proposed. It was
rejected because it is enforced by discipline — every future edit to the
counting loop has to remember the rule — while a separate dict is enforced
by the type. `ledger()` cannot count what it is never handed. The page can
render both; nothing else can confuse them.

WHY THEY MUST NOT COUNT, which is not the argument I expected:
`measured_n` counts criteria the machine answered for all 3,318 companies
UNIFORMLY. Hand entries exist only for companies the reader chose to
research, and a reader only researches names they already noticed. Let
them into the count and the headline starts ranking companies by how much
the reader already likes them — a confirmation-bias amplifier with a
database behind it. This repo already guards the sibling case:
`peer_medians` is computed over the whole universe rather than over
survivors, for exactly this reason.

AND NO BANDS. Neither Mayer nor Phelps gives a cut point for analyst
coverage, work culture or forward PEG. A cut point invented here would
paint a green cell on a page whose premise is not doing that, so a hand
value renders as the number it is, uncoloured. "9 analysts" is a fact
worth seeing; calling it Good is a judgement nobody sourced.

THE ONE EXCEPTION, argued and bounded: criterion 7 may RETRACT its own
veto. It is the only criterion of the six that is not an inference — it is
the company's own number, compelled by the SEC, and this module's regex
has now been wrong about it twice in production. See `retraction` below
for why that exception is safe and why it stops there.
"""
from __future__ import annotations

import checklist as C

# The six a person answers. Deliberately NOT a partition of anything and
# deliberately not one of checklist's four id tuples — those must partition
# 1..13 and are about what the machine can do.
MANUAL_IDS = (6, 7, 8, 9, 10, 12)

# A number with a scale everyone agrees on.
NUMERIC_IDS = (7, 9, 12)
# A judgement with no scale. Forcing these into a number is how a proxy
# for "sound capital allocation" put Intel FY2015-19 in the top band.
NOTE_ONLY_IDS = (6, 8, 10)

UNITS = {7: "% of class", 9: "analysts", 12: "forward PEG"}

# What a human is expected to have open. Shown on the form, stored with
# the answer, and the reason `source` is required rather than optional.
DOCUMENTS = {
    6:  "10-K cash-flow statement, Item 5 buyback table, shareholder letter",
    7:  "DEF 14A — 'Security Ownership' table, the officers-and-directors row",
    8:  "10-K Item 1 and Item 1A, across several years",
    9:  "a broker page or aggregator — count of covering analysts",
    10: "Glassdoor or similar — name the scale you are quoting",
    12: "forward P/E over consensus forward EPS growth",
}

# Criterion 7 is the only one that touches a verdict, in either direction.
RETRACTING_ID = 7

MAX_NOTE = 2000
MAX_SOURCE = 500


class Invalid(ValueError):
    """A hand entry that will not be stored, with a reason for the form."""


def validate(criterion, value=None, note="", source="", read_on="") -> dict:
    """One clean entry, or raise Invalid with something a form can show.

    Refuses rather than coerces. A hand entry is the one input on this page
    that costs a human an hour of reading, and silently accepting a wrong
    unit would waste it in a way nobody would ever notice.
    """
    try:
        cid = int(criterion)
    except (TypeError, ValueError):
        raise Invalid(f"criterion must be a number, got {criterion!r}")
    if cid not in MANUAL_IDS:
        raise Invalid(f"criterion {cid} is not hand-enterable; "
                      f"expected one of {list(MANUAL_IDS)}")

    note = (note or "").strip()
    source = (source or "").strip()
    if not source:
        raise Invalid("say what you read — an unsourced hand entry is the "
                      "same unmeasured number this page exists to expose")
    if len(source) > MAX_SOURCE:
        raise Invalid(f"source is longer than {MAX_SOURCE} characters")
    if len(note) > MAX_NOTE:
        raise Invalid(f"note is longer than {MAX_NOTE} characters")

    num = None
    if cid in NUMERIC_IDS:
        if value is None or value == "":
            raise Invalid(f"criterion {cid} needs a number ({UNITS[cid]})")
        try:
            num = float(value)
        except (TypeError, ValueError):
            raise Invalid(f"criterion {cid} needs a number, got {value!r}")
        if num != num or num in (float("inf"), float("-inf")):
            raise Invalid("not a finite number")
        if cid == 7 and not 0.0 <= num <= 100.0:
            raise Invalid("percent of class is between 0 and 100. If you are "
                          "reading a share COUNT, this is the wrong column")
        if cid == 9 and num < 0:
            raise Invalid("an analyst count cannot be negative")
        if cid == 12 and num <= 0:
            raise Invalid("PEG is undefined at or below zero growth — leave "
                          "it unmeasured rather than entering a negative")
    else:
        # A judgement with no scale. A number here would be invented
        # precision, so it is dropped rather than stored and ignored.
        if not note:
            raise Invalid(f"criterion {cid} has no agreed scale, so it takes "
                          f"a written finding rather than a number")

    return {"criterion": cid, "value": num, "note": note,
            "source": source, "read_on": (read_on or "").strip() or None}


def retraction(row: dict, entry: dict | None) -> tuple[bool, str]:
    """(should the verdict change, why) for criterion 7 on one row.

    THE ONLY PLACE A HAND ENTRY MOVES A VERDICT, in either direction, and
    it is safe for a reason that is a property of the build rather than of
    this function: `evaluate()` runs the vetoes LAST, after a company has
    already cleared both the measured and the passing thresholds. So a row
    rejected with `vetoed_by == [7]` is one whose every other test already
    passed, and cancelling that veto restores exactly the verdict the build
    would have reached. Nothing is recomputed and nothing is inferred.

    It stops at criterion 7 because 7 is the only one of the six that is
    not an inference. It is the company's own figure, compelled by the SEC,
    published in a table with a row label — and the regex reading that
    table has been wrong twice in production, both times in the direction
    of rejecting companies. 6 and 8 have no comparable number: a human
    reading a 10-K knows the share price and is reading prose written by
    the party being graded, which is how every capital-allocation proxy
    tested put Intel FY2015-19 in the top band.

    Symmetric on purpose. If the filing says insiders hold almost nothing,
    the veto fires whether the machine or the reader found it — that
    direction removes a name you researched and so cannot flatter anything.
    """
    if not entry or entry.get("criterion") != RETRACTING_ID:
        return False, ""
    val = entry.get("value")
    if val is None:
        return False, ""
    aligned = val >= C.INSIDER_MIN_PCT
    verdict = row.get("verdict")
    vetoed_by = list((row.get("ledger") or {}).get("vetoed_by") or [])

    if verdict == "reject" and vetoed_by == [RETRACTING_ID] and aligned:
        return True, (f"criterion 7 veto retracted — you read {val:g}% "
                      f"against a {C.INSIDER_MIN_PCT:g}% floor")
    if verdict == "list" and not aligned:
        return True, (f"criterion 7 vetoed by hand — you read {val:g}% "
                      f"against a {C.INSIDER_MIN_PCT:g}% floor")
    return False, ""


def merge(rows: list[dict], entries: dict) -> dict:
    """Attach hand entries to rows and apply the criterion-7 retraction.

    `entries` is {TICKER: {criterion_int: entry}}. Returns a summary; the
    rows are updated in place, which is what the request path wants — the
    snapshot on disk is never touched, so this runs against a file the
    monthly build is free to overwrite.

    Rows gain a `hand` key. They do NOT gain anything inside `measures`,
    and that separation is the whole design: `ledger()` counts `measures`,
    so a hand value is arithmetically incapable of reaching the headline.
    """
    retracted, hand_vetoed, attached = [], [], 0
    for row in rows or []:
        tkr = (row.get("ticker") or "").upper()
        by_criterion = entries.get(tkr)
        if not by_criterion:
            continue
        row["hand"] = {str(cid): e for cid, e in sorted(by_criterion.items())}
        attached += 1
        changed, why = retraction(row, by_criterion.get(RETRACTING_ID))
        if not changed:
            continue
        row["hand_verdict"] = why
        if row.get("verdict") == "reject":
            row["verdict"] = "list"
            row["reason"] = why
            retracted.append(tkr)
        else:
            row["verdict"] = "reject"
            row["reason"] = f"veto: {why}"
            hand_vetoed.append(tkr)
    return {"attached": attached,
            "retracted": sorted(retracted),
            "hand_vetoed": sorted(hand_vetoed),
            "entries": sum(len(v) for v in (entries or {}).values())}


RULES = {
    "manual_ids": list(MANUAL_IDS),
    "numeric_ids": list(NUMERIC_IDS),
    "note_only_ids": list(NOTE_ONLY_IDS),
    "retracting_id": RETRACTING_ID,
    "units": {str(k): v for k, v in UNITS.items()},
    "documents": {str(k): v for k, v in DOCUMENTS.items()},
    "insider_min_pct": C.INSIDER_MIN_PCT,
}

"""Scan the EDGAR daily index for the filings that are actually catalysts.

WHY THE DAILY INDEX AND NOT FULL-TEXT SEARCH: the daily index is a plain
file listing every filing of every form type for a day. It is stable,
documented, cheap (one request per day scanned) and cannot silently
change its ranking under us. Full-text search is better for phrase hunts
but is an undocumented JSON endpoint; it is used here only to resolve the
ambiguous forms, and its absence degrades the run rather than failing it.

WHY CI: sec.gov is unreachable from the dev sandbox, so this cannot be
developed against live data there. The classification logic lives in
catalysts.py where it is unit-tested offline; this file is only the fetch.

The output is a QUEUE, not a feed. For odd-lot tenders and liquidations
the window is days to weeks — hours of latency cost nothing, and a
real-time stream would only encourage the churn that eats the after-tax
return the whole exercise exists to protect.

Usage:  python scripts/refresh_catalysts.py [--days 3] [--dry-run]
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import json
import logging
import sys
import re
import threading
import time
import urllib.error
import urllib.request
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import catalysts as CT                                    # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("catalysts")

OUT = ROOT / "data" / "catalysts.json"
UA = "MarketPulse/1.0 invoice@archfms.com"
IDX = "https://www.sec.gov/Archives/edgar/daily-index/{y}/QTR{q}/form.{ymd}.idx"

# Form prefixes worth pulling out of the index. Kept in sync with the
# catalogue by construction rather than by hand: anything catalysts.py
# knows about is watched, and nothing else is.
WATCHED = tuple(sorted({k.split("/")[0] for k in CT.CATALYSTS}, key=len, reverse=True))


def _get(url: str) -> str | None:
    for attempt in range(3):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=30) as r:
                return r.read().decode("latin-1")
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None                # weekend / holiday — not an error
            if attempt == 2:
                log.warning("HTTP %s on %s", e.code, url)
                return None
            time.sleep(2 ** attempt)
        except (urllib.error.URLError, TimeoutError, OSError):
            if attempt == 2:
                return None
            time.sleep(2 ** attempt)
    return None


def parse_index(text: str, day: str) -> tuple[list[dict], int]:
    """Parse EDGAR's daily form.idx. Returns (catalyst rows, total data rows).

    SPLIT ON RUNS OF TWO-OR-MORE SPACES, not on single whitespace and not
    on column offsets read from the header.

      * Single-whitespace splitting destroys every multi-token form type —
        "SC 13D" becomes "SC", "NT 10-K" becomes "NT" — which is nearly
        the entire catalyst list.
      * Column offsets from the header work until the header is absent,
        reworded, or the file is served as something else entirely. The
        first live run fetched two index files and parsed ZERO rows from
        them, and column parsing gave no way to tell whether the file was
        malformed or simply had no catalysts in it.

    Form types never contain two consecutive spaces, and neither do
    company names in practice — but the columns are padded apart by many.
    So a 2+ space split is both simpler and far harder to break, and it
    needs no header at all.

    The second return value is the count of rows that PARSED, whether or
    not they were catalysts. Fetching a file and parsing nothing out of it
    is a broken feed, not a quiet market — EDGAR carries hundreds of
    Form 4s every weekday — and the caller has to be able to tell those
    apart.
    """
    rows: list[dict] = []
    parsed = 0
    for line in text.splitlines():
        line = line.rstrip()
        # Every data row ends with the archive path. Header, preamble and
        # separator lines do not, which makes this the cheapest possible
        # "is this a row?" test and needs no header detection.
        if "edgar/data/" not in line:
            continue
        parts = re.split(r"\s{2,}", line.strip())
        if len(parts) < 5:
            continue
        form, fname, filed, cik = parts[0], parts[-1], parts[-2], parts[-3]
        if not fname.startswith("edgar/"):
            continue
        parsed += 1
        hit = CT.classify(form)
        if not hit:
            continue
        company = " ".join(parts[1:-3]).strip()
        rows.append({
            "form": form, "company": company, "cik": cik,
            "filed": filed or day,
            "url": f"https://www.sec.gov/Archives/{fname}",
            "accession": fname.rsplit("/", 1)[-1],
            "label": hit["label"], "note": hit["note"],
            "typical_days": hit["typical_days"], "completion": hit["completion"],
            "actionable": hit.get("actionable", True),
            "warning": bool(hit.get("warning")),
            "routine": CT.is_routine_repurchase(form, company),
        })
    return rows, parsed


# ── Stage two: open the Form 4s ─────────────────────────────────────
#
# The index says a Form 4 exists. It cannot say whether an insider BOUGHT,
# and most Form 4s are vesting, option exercises, tax withholding and
# scheduled sales. It also lists one row per filer CIK — issuer plus every
# reporting owner — so one filing by a fund group arrives as ten rows
# carrying the LP names in the company column.
#
# Both problems are solved by the same request: fetch the document once
# per ACCESSION NUMBER (not per row) and read it.
SEC_RATE_LIMIT = 8.0                # requests/second; sec.gov permits 10
FORM4_WORKERS = 4
MAX_FORM4 = 2500                    # a heavy day is ~1,200; this is a runaway guard


class _Throttle:
    """Token bucket shared across worker threads. sec.gov bans on rate."""

    def __init__(self, per_second: float):
        self._gap = 1.0 / per_second
        self._lock = threading.Lock()
        self._next = 0.0

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            due = max(now, self._next)
            self._next = due + self._gap
        if due > now:
            time.sleep(due - now)


def resolve_form4s(rows: list[dict], throttle: _Throttle,
                   cap: int = MAX_FORM4) -> tuple[list[dict], dict]:
    """Turn raw Form 4 index rows into the purchases among them.

    Returns (purchase rows, stats). Everything that is not a code-P
    open-market acquisition is dropped — which on a normal day is the large
    majority — and what survives is keyed to the ISSUER, with the buyer,
    their role, and the dollars they committed.
    """
    by_accession: dict[str, dict] = {}
    for r in rows:
        by_accession.setdefault(r["accession"], r)

    todo = list(by_accession.values())
    stats = {"filings": len(todo), "index_rows": len(rows), "capped": 0,
             "fetch_failed": 0, "unparsed": 0, "not_purchases": 0}
    if len(todo) > cap:
        # Never truncate silently: a shortened queue must not read as a
        # quiet day. Sorted so the cap falls in a stable place, and the
        # count that got dropped is reported in the payload.
        todo.sort(key=lambda r: r["accession"])
        stats["capped"] = len(todo) - cap
        log.warning("Form 4 filings (%d) exceed the cap of %d — skipping %d. "
                    "The queue will be INCOMPLETE and says so.",
                    len(todo), cap, stats["capped"])
        todo = todo[:cap]

    out: list[dict] = []
    lock = threading.Lock()

    def work(row: dict) -> None:
        throttle.wait()
        raw = _get(row["url"])
        if raw is None:
            with lock:
                stats["fetch_failed"] += 1
            return
        doc = CT.parse_form4(raw)
        if not doc:
            with lock:
                stats["unparsed"] += 1
            return
        buy = CT.insider_buy(doc)
        if not buy:
            with lock:
                stats["not_purchases"] += 1
            return
        with lock:
            out.append({**row,
                        "company": doc["issuer"] or row["company"],
                        "cik": doc["issuer_cik"] or row["cik"],
                        "ticker": doc["ticker"],
                        "buy": buy})

    with cf.ThreadPoolExecutor(max_workers=FORM4_WORKERS) as pool:
        list(pool.map(work, todo))

    stats["purchases"] = len(out)
    return out, stats


def merge_clusters(buys: list[dict]) -> list[dict]:
    """Collapse same-issuer purchases into one row.

    Several insiders buying the same stock in the same window is a
    materially different observation from one director topping up, and it
    is the one that empirically carries signal. Reporting them as separate
    rows both buries the pattern and inflates the queue.
    """
    grouped: dict[str, dict] = {}
    for r in sorted(buys, key=lambda r: r["filed"], reverse=True):
        g = grouped.get(r["cik"])
        if g is None:
            # Copy the nested lists, not just the dict. Merging mutates
            # roles/buyers in place, and a shallow copy would write those
            # edits back into the source row.
            buy = {**r["buy"], "roles": list(r["buy"]["roles"]),
                   "buyers": list(r["buy"]["buyers"])}
            grouped[r["cik"]] = {**r, "buy": buy, "filings": 1,
                                 "buyers": list(buy["buyers"])}
            continue
        g["filings"] += 1
        b, nb = g["buy"], r["buy"]
        b["shares"] = round(b["shares"] + nb["shares"], 2)
        if nb["dollars"]:
            b["dollars"] = round((b["dollars"] or 0) + nb["dollars"], 2)
        b["unpriced_shares"] = round(b["unpriced_shares"] + nb["unpriced_shares"], 2)
        b["fully_priced"] = b["fully_priced"] and nb["fully_priced"]
        for name in nb["buyers"]:
            if name not in g["buyers"]:
                g["buyers"].append(name)
        for role in nb["roles"]:
            if role not in b["roles"]:
                b["roles"].append(role)
        b["by_insider"] = b["by_insider"] or nb["by_insider"]

    rows = []
    for g in grouped.values():
        b = g["buy"]
        b["buyers"] = g["buyers"]
        g["distinct_buyers"] = len(g["buyers"])
        g["cluster"] = g["distinct_buyers"] > 1
        g.pop("buyers", None)
        # Recompute after merging: the per-share figure and the sanity
        # check both describe the COMBINED position, and a merge of two
        # sane filings can still land somewhere impossible.
        priced = b["shares"] - b["unpriced_shares"]
        b["price_per_share"] = (round(b["dollars"] / priced, 4)
                                if b["dollars"] and priced > 0 else None)
        b["suspect"] = CT.implausible(priced, b["dollars"])
        rows.append(g)

    # Biggest commitment first — but a row whose arithmetic does not
    # describe a trade must never lead the board. The first live run put
    # "$23,649,660,000" at the top on an implied $180,000 a share; sorting
    # by size alone guarantees that the least trustworthy number gets the
    # most prominent position. Suspect rows keep their place in the list
    # and lose their claim on the top of it.
    #
    # Unpriced buys are ranked last among the clean rows rather than as $0:
    # unknown is not small.
    rows.sort(key=lambda r: (r["buy"]["suspect"] is None,
                             r["buy"]["dollars"] is not None,
                             r["buy"]["dollars"] or 0), reverse=True)
    return rows


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=4, help="trailing days of index to scan")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--skip-form4", action="store_true",
                    help="skip the per-filing Form 4 pass (deal filings only)")
    a = ap.parse_args()

    all_rows: list[dict] = []
    scanned = 0
    total_parsed = 0
    for back in range(a.days):
        d = date.today() - timedelta(days=back)
        if d.weekday() >= 5:               # EDGAR does not publish weekends
            continue
        url = IDX.format(y=d.year, q=(d.month - 1) // 3 + 1, ymd=d.strftime("%Y%m%d"))
        text = _get(url)
        if not text:
            log.info("no index for %s (holiday, or not posted yet)", d)
            continue
        scanned += 1
        found, parsed = parse_index(text, d.isoformat())
        total_parsed += parsed
        log.info("%s: %d rows parsed, %d catalysts", d, parsed, len(found))
        if parsed == 0:
            # Fetched something and understood none of it. Show what arrived
            # so the next fix is made from evidence rather than a guess.
            head = [ln for ln in text.splitlines() if ln.strip()][:6]
            log.error("Parsed NOTHING from %s (%d bytes). First lines received:", d, len(text))
            for ln in head:
                log.error("    %r", ln[:160])
        all_rows.extend(found)
        time.sleep(0.3)                    # be polite to sec.gov

    if scanned == 0:
        log.error("Scanned no index files at all — refusing to overwrite the queue "
                  "with an empty one. An empty board must never read as 'nothing "
                  "is happening' when it means 'the fetch failed'.")
        return 1
    # Fetching files and parsing nothing out of them is a broken feed, not a
    # quiet market: EDGAR carries hundreds of Form 4s every weekday. The first
    # live run did exactly this and reported success, which is the failure
    # this guard exists to make impossible.
    if total_parsed == 0:
        log.error("Fetched %d index file(s) but parsed 0 rows from them. That is a "
                  "format or delivery problem, not an absence of filings — refusing "
                  "to write. See the sample lines above.", scanned)
        return 1

    # Split the Form 4s out. They are ~97% of the index hits and cannot be
    # judged from the index at all, so they get their own resolution pass.
    raw_f4 = [r for r in all_rows if r["form"].upper().startswith("4")]
    others = [r for r in all_rows if not r["form"].upper().startswith("4")]

    f4_stats: dict = {"filings": 0, "index_rows": 0, "purchases": 0}
    insider_rows: list[dict] = []
    if raw_f4 and not a.skip_form4:
        log.info("Resolving %d Form 4 index rows (%d distinct filings)...",
                 len(raw_f4), len({r["accession"] for r in raw_f4}))
        t0 = time.monotonic()
        buys, f4_stats = resolve_form4s(raw_f4, _Throttle(SEC_RATE_LIMIT))
        insider_rows = merge_clusters(buys)
        log.info("Form 4: %d filings read in %.0fs → %d purchases → %d issuers "
                 "(%d not purchases, %d unreadable, %d fetch failures)",
                 f4_stats["filings"], time.monotonic() - t0, f4_stats["purchases"],
                 len(insider_rows), f4_stats["not_purchases"],
                 f4_stats["unparsed"], f4_stats["fetch_failed"])
        if f4_stats["filings"] and not f4_stats["purchases"]:
            # Possible on a genuinely quiet day, but also exactly what a
            # changed document format looks like. Say which is unproven.
            log.warning("Read %d Form 4s and found NO open-market purchases. That "
                        "happens on quiet days, but it is also what a format change "
                        "looks like — check one filing by hand before trusting it.",
                        f4_stats["filings"])

    # De-duplicate the deal filings. Amendments to the same filing land
    # repeatedly; keyed on (cik, form) so SC TO-I and its /A stay distinct.
    seen, rows = set(), []
    for r in sorted(others, key=lambda r: (r["filed"], r["company"]), reverse=True):
        key = (r["cik"], r["form"])
        if key in seen:
            continue
        seen.add(key)
        rows.append(r)

    routine = [r for r in rows if r.get("routine")]
    rows = [r for r in rows if not r.get("routine")]
    if routine:
        log.info("Set aside %d scheduled fund/BDC repurchase offers", len(routine))

    # Warnings first — an exit alarm on something you hold beats a new idea.
    rows.sort(key=lambda r: (not r["warning"], r["filed"]), reverse=False)
    rows.sort(key=lambda r: r["warning"], reverse=True)

    blob = {
        "_meta": {
            "as_of": date.today().isoformat(),
            "days_scanned": scanned,
            "rows_parsed": total_parsed,
            "filings_scanned": len(all_rows),
            "matched": len(rows),
            "insider_issuers": len(insider_rows),
            "routine_set_aside": len(routine),
            "form4": f4_stats,
            "watched_forms": list(WATCHED),
            "note": ("A queue, not a feed. Deal terms — price, consideration, deadline — "
                     "come from reading the filing; nothing here scrapes them, because a "
                     "wrong tender price is worse than no tender price. Form 4s ARE read: "
                     "only transaction code P survives, keyed to the issuer, ranked by "
                     "dollars committed."),
        },
        "rows": rows,
        "insiders": insider_rows,
        "routine": routine,
    }
    if a.dry_run:
        log.info("dry run — %d deal rows, %d insider issuers, not writing",
                 len(rows), len(insider_rows))
        return 0
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(blob, indent=1))
    log.info("Wrote %s — %d deal filings, %d insider issuers, %d routine set aside "
             "(from %d index days, %d raw index hits)",
             OUT, len(rows), len(insider_rows), len(routine), scanned, len(all_rows))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

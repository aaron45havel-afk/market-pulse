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
import json
import logging
import sys
import re
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
        rows.append({
            "form": form, "company": " ".join(parts[1:-3]).strip(), "cik": cik,
            "filed": filed or day,
            "url": f"https://www.sec.gov/Archives/{fname}",
            "label": hit["label"], "note": hit["note"],
            "typical_days": hit["typical_days"], "completion": hit["completion"],
            "actionable": hit.get("actionable", True),
            "warning": bool(hit.get("warning")),
        })
    return rows, parsed


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=4, help="trailing days of index to scan")
    ap.add_argument("--dry-run", action="store_true")
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

    # De-duplicate: amendments to the same filing land repeatedly.
    seen, rows = set(), []
    for r in sorted(all_rows, key=lambda r: (r["filed"], r["company"]), reverse=True):
        key = (r["cik"], r["form"])
        if key in seen:
            continue
        seen.add(key)
        rows.append(r)
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
            "watched_forms": list(WATCHED),
            "note": ("A queue, not a feed. Deal terms — price, consideration, deadline — "
                     "come from reading the filing; nothing here scrapes them, because a "
                     "wrong tender price is worse than no tender price."),
        },
        "rows": rows,
    }
    if a.dry_run:
        log.info("dry run — %d rows, not writing", len(rows))
        return 0
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(blob, indent=1))
    log.info("Wrote %s (%d rows from %d index days)", OUT, len(rows), scanned)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

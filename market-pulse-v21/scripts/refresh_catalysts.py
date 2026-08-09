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


def parse_index(text: str, day: str) -> list[dict]:
    """Parse EDGAR's daily form.idx.

    IT IS FIXED-WIDTH, AND THAT MATTERS. Splitting on whitespace looks
    like it works and silently destroys every multi-token form type —
    "SC 13D" becomes "SC", "NT 10-K" becomes "NT" — which is nearly the
    entire catalyst list. Company names are multi-word too, so the tail
    cannot be counted from the right either.

    So the column offsets are read off the header line EDGAR provides,
    and every row is sliced at those positions.
    """
    rows: list[dict] = []
    lines = text.splitlines()
    cols: dict[str, int] | None = None
    started = False

    for line in lines:
        if cols is None and "Form Type" in line and "CIK" in line and "File Name" in line:
            cols = {name: line.index(name)
                    for name in ("Form Type", "Company Name", "CIK", "Date Filed", "File Name")
                    if name in line}
            if len(cols) < 5:
                cols = None
            continue
        if line.startswith("---"):
            started = True
            continue
        if not started or not cols or not line.strip():
            continue

        def col(name: str, nxt: str | None) -> str:
            start = cols[name]
            end = cols[nxt] if nxt else len(line)
            return line[start:end].strip()

        form = col("Form Type", "Company Name")
        company = col("Company Name", "CIK")
        cik = col("CIK", "Date Filed")
        filed = col("Date Filed", "File Name")
        fname = col("File Name", None)
        if not form or not fname:
            continue
        hit = CT.classify(form)
        if not hit:
            continue
        rows.append({
            "form": form, "company": company, "cik": cik,
            "filed": filed or day,
            "url": f"https://www.sec.gov/Archives/{fname}",
            "label": hit["label"], "note": hit["note"],
            "typical_days": hit["typical_days"], "completion": hit["completion"],
            "actionable": hit.get("actionable", True),
            "warning": bool(hit.get("warning")),
        })
    return rows


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=4, help="trailing days of index to scan")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()

    all_rows: list[dict] = []
    scanned = 0
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
        found = parse_index(text, d.isoformat())
        log.info("%s: %d catalyst filings", d, len(found))
        all_rows.extend(found)
        time.sleep(0.3)                    # be polite to sec.gov

    if scanned == 0:
        log.error("Scanned no index files at all — refusing to overwrite the queue "
                  "with an empty one. An empty board must never read as 'nothing "
                  "is happening' when it means 'the fetch failed'.")
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

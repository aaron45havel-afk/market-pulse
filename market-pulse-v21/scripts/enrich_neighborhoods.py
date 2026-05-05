"""Reverse-geocode each ZIP centroid to get a neighborhood / locality
name and persist the cache in ``data/zip_neighborhoods.json``.

Why: USPS / Census / Zillow only resolve to city level, so popups for
all 30+ ZIPs in Columbus all show "Columbus, OH". OSM-derived sources
have neighborhood-level tags ("Short North" for 43201, "Bexley" for
43209, etc.). This script queries Photon (Komoot's free OSM reverse-
geocoder, https://photon.komoot.io) and walks the result for the
most-specific locality name available.

Cache strategy: write-once per ZIP. Subsequent runs only enrich ZIPs
that don't have a cached value, so the file grows toward 30K entries
across multiple runs. Once full, refreshes are near-instant.

Rate limit: 5 requests/second (200ms sleep). Photon doesn't publish a
hard rate limit but asks for fair use; 5/s is conservative and gets
all 30K ZIPs in ~1.7 hours when run end-to-end.

Runtime cap: 5 hours by default — fits inside GitHub Actions' 6-hour
job limit. Partial runs persist the cache after every 100 ZIPs so a
cap-out resumes cleanly on the next run.

Usage:
    python scripts/enrich_neighborhoods.py [--max-runtime SECONDS] [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import logging
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from datetime import date
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = REPO_ROOT / "data" / "zips.db"
CACHE_PATH = REPO_ROOT / "data" / "zip_neighborhoods.json"

PHOTON_URL = "https://photon.komoot.io/reverse"
RATE_LIMIT_SLEEP = 0.21       # 5/sec, slightly slower than max
CHECKPOINT_EVERY = 100        # persist cache every N enriched ZIPs


def fetch_neighborhood(lat: float, lng: float) -> str | None:
    """Reverse-geocode lat/lng via Photon, return the most-specific
    locality name we can find. Walks Photon's properties dict in
    decreasing specificity (neighbourhood > suburb > district >
    locality), falling back to None when nothing is tagged.
    """
    url = f"{PHOTON_URL}?lat={lat}&lon={lng}&lang=en"
    req = urllib.request.Request(url, headers={"User-Agent": "market-pulse/1 (national-zips enrichment)"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read())
    except urllib.error.HTTPError as e:
        log.warning("  Photon HTTP %s for %s,%s", e.code, lat, lng)
        return None
    except urllib.error.URLError as e:
        log.warning("  Photon network error %s,%s: %s", lat, lng, e.reason)
        return None
    except (json.JSONDecodeError, ValueError) as e:
        log.warning("  Photon parse error %s,%s: %s", lat, lng, e)
        return None
    feats = data.get("features", []) if isinstance(data, dict) else []
    if not feats:
        return None
    # OSM tags vary per feature. Walk in decreasing specificity.
    for feat in feats[:3]:   # check up to top 3 results
        props = (feat or {}).get("properties", {}) or {}
        for key in ("neighbourhood", "suburb", "district", "locality"):
            val = props.get(key)
            if val and isinstance(val, str) and val.strip():
                return val.strip()
    return None


def load_cache() -> dict:
    """Load existing cache, returning {zip: neighborhood_name}. On
    parse failure or missing file, returns empty cache."""
    if not CACHE_PATH.exists():
        return {}
    try:
        payload = json.loads(CACHE_PATH.read_text())
    except (json.JSONDecodeError, OSError):
        return {}
    return payload.get("neighborhoods", {}) or {}


def write_cache(cache: dict) -> None:
    payload = {
        "_meta": {
            "as_of": date.today().isoformat(),
            "source": "Photon (komoot.io OSM reverse-geocoder), neighbourhood/suburb/district tags",
            "count": len(cache),
        },
        "neighborhoods": cache,
    }
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    CACHE_PATH.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument(
        "--max-runtime", type=int, default=18000,
        help="Stop after this many seconds (default 18000 = 5 hours).",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Identify pending ZIPs and exit without making any Photon calls.",
    )
    parser.add_argument(
        "--limit", type=int, default=0,
        help="Process at most N pending ZIPs this run (0 = all up to time cap).",
    )
    args = parser.parse_args(argv)

    if not DB_PATH.exists():
        log.error("zips.db missing at %s — run build_national_zips.py first.", DB_PATH)
        return 1

    cache = load_cache()
    log.info("Cache currently has %d ZIPs", len(cache))

    # Read full ZIP list + lat/lng from the DB. We also persist any
    # NULL/empty cache entries so a failed lookup gets retried next
    # run instead of being silently skipped — but we DON'T re-enrich
    # ZIPs that already have a non-empty value (they're stable).
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT zip, lat, lng FROM zips ORDER BY zip").fetchall()
    conn.close()

    pending = [(r["zip"], r["lat"], r["lng"])
               for r in rows
               if r["lat"] is not None and r["lng"] is not None
               and not (cache.get(r["zip"]) or "").strip()]
    log.info("Found %d ZIPs without a cached neighborhood (of %d total)",
             len(pending), len(rows))

    if args.dry_run:
        log.info("--dry-run: would enrich up to %d ZIPs", len(pending))
        return 0
    if not pending:
        log.info("Nothing to do.")
        return 0

    start = time.monotonic()
    enriched = 0
    for i, (zcode, lat, lng) in enumerate(pending, 1):
        if time.monotonic() - start > args.max_runtime:
            log.info("Hit --max-runtime cap, stopping at %d/%d.", i - 1, len(pending))
            break
        if args.limit and enriched >= args.limit:
            log.info("Hit --limit cap, stopping at %d.", enriched)
            break
        name = fetch_neighborhood(lat, lng)
        if name:
            cache[zcode] = name
            enriched += 1
        # Sleep before the next request even on miss — be polite.
        time.sleep(RATE_LIMIT_SLEEP)
        if enriched and enriched % CHECKPOINT_EVERY == 0:
            write_cache(cache)
            log.info("  checkpoint @ %d enriched (%d total cached)", enriched, len(cache))

    write_cache(cache)
    elapsed = int(time.monotonic() - start)
    log.info("Done. Enriched %d this run · %d cached total · %ds elapsed",
             enriched, len(cache), elapsed)
    return 0


if __name__ == "__main__":
    sys.exit(main())

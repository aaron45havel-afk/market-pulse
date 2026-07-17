"""Monthly condo-segment pricing + long-window steadiness for /norcal.

The buyable asset in the strict-gate Bay towns is a condo/townhome, but
zips.db carries only the all-homes median and 60 months of history —
enough to rank quality, not enough to (a) price the buyable segment or
(b) prove steadiness through a full cycle (you want 2008 in view).

This pulls Zillow's public condo-tier ZHVI CSV (ZIP-level, monthly back
to 2000), filters to the /norcal Bay universe, and writes
data/norcal_condo.json:

    {"as_of": "2026-06", "series": {
        "94061": {"price": 742000, "cagr15": 4.1, "vol": 5.2,
                   "max_dd": -24.3, "n_months": 312}, ...}}

norcal.screen() prefers this overlay per-ZIP: entry price becomes the
condo ZHVI and the steadiness gate runs on the 15-yr condo series
itself (condos are the Bay's boom-bust-prone segment — judging them by
the SFH-flattered ZIP median would be exactly the mistake the gate
exists to prevent). Missing/malformed file → all-homes fallback.

Source CSV ≈ 30-60 MB; streamed row-by-row, only Bay rows kept.
Cadence: 8th of each month.
"""
from __future__ import annotations

import csv
import io
import json
import statistics
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

CSV_URL = ("https://files.zillowstatic.com/research/public_csvs/zhvi/"
           "Zip_zhvi_uc_condo_tier_0.33_0.67_sm_sa_month.csv")
HEADERS = {"User-Agent": "market-pulse-refresh/1.0"}
WINDOW_YEARS = 15
MIN_MONTHS = 96          # need ≥8 yrs of condo history to say anything


def bay_zips() -> set[str]:
    from norcal import _universe, _ZIPS_DB
    import sqlite3
    if not _ZIPS_DB.exists():
        print("[norcal] zips.db missing — cannot resolve the Bay universe.")
        return set()
    conn = sqlite3.connect(str(_ZIPS_DB))
    try:
        return {r["zip"] for r in _universe(conn)}
    finally:
        conn.close()


def stats_from_series(vals: list[float]) -> dict | None:
    vals = [v for v in vals if v and v > 0]
    if len(vals) < MIN_MONTHS:
        return None
    window = vals[-WINDOW_YEARS * 12:]
    years = (len(window) - 1) / 12
    cagr = ((window[-1] / window[0]) ** (1 / years) - 1) * 100
    yoy = [(window[i] / window[i - 12] - 1) * 100
           for i in range(12, len(window)) if window[i - 12] > 0]
    vol = statistics.pstdev(yoy) if len(yoy) >= 12 else None
    peak, max_dd = window[0], 0.0
    for v in window:
        peak = max(peak, v)
        max_dd = min(max_dd, (v / peak - 1) * 100)
    return {"price": round(window[-1]),
            "cagr15": round(cagr, 1),
            "vol": round(vol, 1) if vol is not None else None,
            "max_dd": round(max_dd, 1),
            "n_months": len(window)}


def main() -> int:
    targets = bay_zips()
    if not targets:
        return 2
    print(f"[norcal] CA universe: {len(targets)} ZIPs. Downloading Zillow condo ZHVI…")
    try:
        with urllib.request.urlopen(
                urllib.request.Request(CSV_URL, headers=HEADERS), timeout=300) as r:
            text = io.TextIOWrapper(r, encoding="utf-8", errors="replace")
            reader = csv.reader(text)
            header = next(reader)
            try:
                zip_idx = header.index("RegionName")
            except ValueError:
                print("[norcal] CSV header changed — no RegionName column. "
                      f"Got: {header[:12]}")
                return 5
            # Date columns are everything that parses as YYYY-MM-DD.
            date_cols = [(i, h) for i, h in enumerate(header)
                         if len(h) == 10 and h[4] == "-" and h[:4].isdigit()]
            if len(date_cols) < MIN_MONTHS:
                print(f"[norcal] Only {len(date_cols)} date columns — format changed.")
                return 5
            as_of = date_cols[-1][1][:7]
            out: dict[str, dict] = {}
            scanned = 0
            for row in reader:
                scanned += 1
                z = row[zip_idx].zfill(5) if zip_idx < len(row) else ""
                if z not in targets:
                    continue
                vals = []
                for i, _ in date_cols:
                    try:
                        vals.append(float(row[i]) if i < len(row) and row[i] else None)
                    except ValueError:
                        vals.append(None)
                s = stats_from_series([v for v in vals if v is not None])
                if s:
                    out[z] = s
    except OSError as e:
        print(f"[norcal] Download failed: {e}")
        return 3

    print(f"[norcal] Scanned {scanned} rows → {len(out)} CA ZIPs with usable condo series")
    if len(out) < 20:
        print("[norcal] Too few — refusing to overwrite with a bad run.")
        return 4

    payload = {
        "as_of": as_of,
        "fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%MZ"),
        "window_years": WINDOW_YEARS,
        "series": out,
    }
    out_path = Path(__file__).resolve().parent.parent / "data" / "norcal_condo.json"
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=1, sort_keys=True)
        fh.write("\n")
    print(f"[norcal] ✓ Wrote {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

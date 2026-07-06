"""Weekly audit: is every auto-refreshed data source actually fresh?

Each refresh_*.py script writes a timestamp (``_meta.as_of``, or
``fetched_at`` for OECD) into its output file. This script reads every
one of those timestamps back and flags anything older than its expected
refresh cadence — which catches both "the workflow stopped running" and
the quieter failure mode where the workflow reports success but the
commit step silently no-ops (see the `git diff` vs `git diff --staged`
bug that discarded every growth_overrides.json refresh from May-July
2026: the file was untracked, so `git diff --quiet` never saw a change).

Usage:
    python scripts/check_data_freshness.py

Exits 1 (and prints a report) if anything is missing or stale. Exits 0
if every source is within its expected age. Run weekly via
.github/workflows/check-data-freshness.yml.
"""
from __future__ import annotations

import json
import sqlite3
import sys
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"


@dataclass
class Source:
    name: str
    workflow: str
    max_age_days: int
    as_of: callable  # () -> str | None (ISO date, "YYYY-MM" or "YYYY-MM-DD"), or None if the file doesn't exist yet


def _meta_as_of(filename: str, key: str = "as_of") -> callable:
    def _read() -> str | None:
        path = DATA_DIR / filename
        if not path.exists():
            return None
        try:
            payload = json.loads(path.read_text())
        except (json.JSONDecodeError, OSError):
            return None
        return payload.get("_meta", {}).get(key) or payload.get(key)
    return _read


def _latest_snapshot_as_of(dirname: str) -> callable:
    def _read() -> str | None:
        snap_dir = DATA_DIR / dirname
        if not snap_dir.is_dir():
            return None
        files = sorted(snap_dir.glob("*.json"))
        if not files:
            return None
        try:
            payload = json.loads(files[-1].read_text())
        except (json.JSONDecodeError, OSError):
            return None
        return payload.get("_meta", {}).get("as_of")
    return _read


def _zips_db_as_of() -> str | None:
    path = DATA_DIR / "zips.db"
    if not path.exists():
        return None
    try:
        conn = sqlite3.connect(str(path))
        row = conn.execute("SELECT MAX(as_of) FROM zips").fetchone()
        conn.close()
        return row[0] if row else None
    except sqlite3.Error:
        return None


SOURCES: list[Source] = [
    Source("Zillow ZHVI/ZORI", "refresh-zillow.yml", 45, _meta_as_of("zillow_overrides.json")),
    Source("Redfin market tracker", "refresh-redfin.yml", 45, _meta_as_of("redfin_overrides.json")),
    Source("Growth outlook (permits + jobs)", "refresh-growth.yml", 45, _meta_as_of("growth_overrides.json")),
    Source("National ZIPs DB", "refresh-national-zips.yml", 45, _zips_db_as_of),
    Source("Lynch GARP screener snapshot", "refresh-lynch-screener.yml", 45, _latest_snapshot_as_of("lynch_snapshots")),
    Source("Net-net screener snapshot", "refresh-screener.yml", 45, _latest_snapshot_as_of("screener_snapshots")),
    Source("BLS state unemployment", "refresh-bls.yml", 45, _meta_as_of("bls_overrides.json")),
    Source("OECD CLI", "refresh-oecd-cli.yml", 45, _meta_as_of("oecd_cli.json", key="fetched_at")),
    Source("Mortgage rate", "refresh-rates.yml", 14, _meta_as_of("rates.json")),
    Source("ZIP neighborhoods (Photon/OSM)", "refresh-neighborhoods.yml", 14, _meta_as_of("zip_neighborhoods.json")),
    Source("Census ACS state demographics", "refresh-census-acs-state.yml", 400, _meta_as_of("census_acs_state_overrides.json")),
]


def _parse_as_of(value: str) -> date | None:
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%MZ", "%Y-%m"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


def main() -> int:
    today = date.today()
    stale: list[str] = []
    ok: list[str] = []

    for src in SOURCES:
        raw = src.as_of()
        if raw is None:
            stale.append(f"- **{src.name}** — data file missing entirely (workflow: `{src.workflow}`)")
            continue
        parsed = _parse_as_of(raw)
        if parsed is None:
            stale.append(f"- **{src.name}** — as_of `{raw}` is unparseable (workflow: `{src.workflow}`)")
            continue
        age_days = (today - parsed).days
        if age_days > src.max_age_days:
            stale.append(
                f"- **{src.name}** — last refreshed {raw} ({age_days}d ago, "
                f"expected within {src.max_age_days}d) (workflow: `{src.workflow}`)"
            )
        else:
            ok.append(f"- {src.name} — {raw} ({age_days}d ago)")

    print(f"Data freshness check — {today.isoformat()}")
    print(f"\n{len(ok)}/{len(SOURCES)} sources fresh.\n")
    if ok:
        print("OK:")
        print("\n".join(ok))
    if stale:
        print("\nSTALE:")
        print("\n".join(stale))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

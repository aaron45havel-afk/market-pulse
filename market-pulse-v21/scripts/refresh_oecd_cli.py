"""Monthly refresh of OECD Composite Leading Indicators for /global-values.

Pulls the amplitude-adjusted CLI series (code: LOLITOAA) for the countries
in country_data.COUNTRIES and writes them to data/oecd_cli.json. The
web page reads that overlay at request time — if it exists, the overlay
values win; if it doesn't (fresh clone, network error, etc.), the
hard-coded snapshot in country_data.COUNTRIES is used.

Output shape:

    {
      "as_of": "2026-05",           # OECD publication month
      "fetched_at": "2026-06-15T04:12Z",
      "series": {
        "US": {"value": 100.4, "prev": 100.2, "trend": "rising"},
        ...
      }
    }

`trend` is derived from the 3-month change vs prev:
  > 0.1  → "rising"
  < -0.1 → "falling"
  otherwise "flat"

Cadence: 15th of each month (OECD publishes CLI ~10th; we buffer 5 days).

Notes for future maintenance:
- OECD's API URL structure has changed multiple times (a hardcoded
  dataflow version 404'd once they bumped it). The fetcher now tries a
  ranked list of flowRefs (_FLOWREFS) — versionless "latest" first, then
  specific versions — and logs which one won. If ALL fail, the run logs
  every attempt and its HTTP status; read that, then check
  https://data-explorer.oecd.org/ → CLI dataset → "Developer API" for the
  current flowRef and add it to the top of _FLOWREFS.
- ISO2 → OECD 3-letter code map is inline below.
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path


# Our internal country codes → OECD 3-letter codes.
CODE_MAP: dict[str, str] = {
    "US": "USA", "JP": "JPN", "UK": "GBR", "DE": "DEU", "FR": "FRA",
    "CA": "CAN", "CH": "CHE", "AU": "AUS", "NL": "NLD", "SE": "SWE",
    "ES": "ESP", "IT": "ITA", "HK": "HKG", "SG": "SGP",
    "KR": "KOR", "TW": "TWN", "CN": "CHN", "IN": "IND", "BR": "BRA",
    "MX": "MEX", "TH": "THA", "ID": "IDN", "ZA": "ZAF", "TR": "TUR",
    "PL": "POL", "MY": "MYS", "PH": "PHL", "CL": "CHL",
}

# Reverse for parsing OECD response.
OECD_TO_OUR = {v: k for k, v in CODE_MAP.items()}


# OECD periodically bumps the CLI dataflow version, and a hardcoded
# version starts returning 404 the moment they do (this is exactly what
# broke the feed — ",4.0" 404'd once they moved on). Rather than pin one
# version, we try a ranked list of flowRefs and use the first that
# returns real data:
#   • a versionless flowRef, which SDMX resolves to the LATEST version
#     (future-proof — survives the next bump with no code change),
#   • then specific versions as fallbacks in case versionless is rejected.
# main() logs which candidate won, so a future break is a one-line fix
# (read the log, add the new flowRef to the top of this list).
_FLOWREFS = (
    "OECD.SDD.STES,DSD_STES@DF_CLI",        # versionless → latest
    "OECD.SDD.STES,DSD_STES@DF_CLI,4.1",
    "OECD.SDD.STES,DSD_STES@DF_CLI,4.0",
    "OECD.SDD.STES,DSD_STES@DF_CLI,5.0",
    "OECD.SDD.STES,DSD_STES@DF_CLI,1.0",
)


def _candidate_urls() -> list[str]:
    """Ranked SDMX-JSON URLs for the amplitude-adjusted CLI (LOLITOAA),
    monthly, for all countries in our set. See _FLOWREFS above."""
    countries = "+".join(CODE_MAP.values())
    tail = (
        f"/{countries}.M.LOLITOAA......"
        "?startPeriod=2024-01"
        "&dimensionAtObservation=AllDimensions"
        "&format=jsondata"
    )
    return [f"https://sdmx.oecd.org/public/rest/data/{fr}{tail}"
            for fr in _FLOWREFS]


def _fetch(url: str, *, timeout: int = 40) -> dict:
    req = urllib.request.Request(
        url,
        headers={
            "Accept":      "application/vnd.sdmx.data+json;version=1.0.0",
            "User-Agent":  "market-pulse-refresh/1.0",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def _extract_series(payload: dict) -> tuple[str, dict[str, dict[str, float]]]:
    """Return (as_of, {'USA': {'2026-05': 100.4, '2026-04': 100.2}, ...})
    from an SDMX-JSON payload. Layout follows OECD Data Explorer's newer
    schema (dimensionAtObservation=AllDimensions)."""
    if "data" not in payload:
        raise RuntimeError("OECD response missing 'data' key — API format changed")
    data = payload["data"]
    struct = data.get("structure") or payload.get("structure") or {}
    dims_obs = struct.get("dimensions", {}).get("observation", [])
    # Find the REF_AREA dimension index (usually 0) and TIME_PERIOD (usually last).
    ref_area_idx = None
    time_idx = None
    ref_area_values = []
    time_values = []
    for i, d in enumerate(dims_obs):
        if d.get("id") == "REF_AREA":
            ref_area_idx = i
            ref_area_values = [v.get("id") for v in d.get("values", [])]
        elif d.get("id") == "TIME_PERIOD":
            time_idx = i
            time_values = [v.get("id") for v in d.get("values", [])]
    if ref_area_idx is None or time_idx is None:
        raise RuntimeError(
            f"OECD response missing REF_AREA / TIME_PERIOD dims (got dims: "
            f"{[d.get('id') for d in dims_obs]})"
        )

    # A present-but-empty "dataSets": [] (a plausible "no data for this
    # query" response) would make [0] raise IndexError; guard it so main()
    # reaches its graceful "zero series" path instead of a red traceback.
    datasets = data.get("dataSets") or []
    obs_dict = datasets[0].get("observations", {}) if datasets else {}
    series: dict[str, dict[str, float]] = {}
    for key, values in obs_dict.items():
        parts = key.split(":")
        try:
            ref_area = ref_area_values[int(parts[ref_area_idx])]
            time_id  = time_values[int(parts[time_idx])]
            value    = values[0]
        except (IndexError, ValueError):
            continue
        if value is None:
            continue
        series.setdefault(ref_area, {})[time_id] = float(value)

    # Determine as_of (most recent common time across countries).
    all_times = sorted({t for s in series.values() for t in s.keys()}, reverse=True)
    as_of = all_times[0] if all_times else ""
    return as_of, series


def _trend(current: float, prev: float | None) -> str:
    if prev is None:
        return "flat"
    diff = current - prev
    if diff > 0.1:  return "rising"
    if diff < -0.1: return "falling"
    return "flat"


def main() -> int:
    candidates = _candidate_urls()
    print(f"[oecd] Fetching CLI from OECD SDMX API "
          f"(trying {len(candidates)} candidate flowRefs)…")

    as_of = None
    series: dict[str, dict[str, float]] = {}
    last_diag = "no attempts made"
    for i, url in enumerate(candidates, 1):
        print(f"[oecd] Attempt {i}/{len(candidates)}: {url}")
        try:
            payload = _fetch(url)
        except urllib.error.HTTPError as e:
            last_diag = f"HTTP {e.code} {e.reason}"
            print(f"[oecd]   → {last_diag}")
            continue
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            last_diag = f"network error: {e}"
            print(f"[oecd]   → {last_diag}")
            continue
        except json.JSONDecodeError as e:
            last_diag = f"response wasn't JSON: {e}"
            print(f"[oecd]   → {last_diag}")
            continue
        try:
            as_of, series = _extract_series(payload)
        except RuntimeError as e:
            last_diag = f"parse error: {e}"
            print(f"[oecd]   → {last_diag}")
            continue
        if not series:
            last_diag = "200 OK but zero series (key/structure mismatch)"
            print(f"[oecd]   → {last_diag}")
            continue
        print(f"[oecd]   → OK: {len(series)} series, latest period {as_of}")
        break

    if not series:
        print(f"[oecd] All {len(candidates)} candidates failed "
              f"(last: {last_diag}).")
        print("[oecd] OECD changed the CLI API beyond the versions tried. "
              "Open https://data-explorer.oecd.org/ → Composite Leading "
              "Indicators → 'Developer API' for the current flowRef, and add "
              "it to the top of _FLOWREFS in this script.")
        return 2

    out: dict[str, dict[str, float | str]] = {}
    for oecd_code, ts in series.items():
        our_code = OECD_TO_OUR.get(oecd_code)
        if not our_code:
            continue
        times = sorted(ts.keys(), reverse=True)
        if not times:
            continue
        current = ts[times[0]]
        prev    = ts[times[1]] if len(times) > 1 else None
        out[our_code] = {
            "value": round(current, 2),
            "prev":  round(prev, 2) if prev is not None else None,
            "trend": _trend(current, prev),
        }

    if not out:
        print("[oecd] No overlapping countries between OECD response and our set.")
        return 7

    result = {
        "as_of":      as_of,
        "fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%MZ"),
        "series":     out,
    }

    data_dir = Path(__file__).resolve().parent.parent / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    out_path = data_dir / "oecd_cli.json"
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(result, fh, indent=2, sort_keys=True)
        fh.write("\n")

    print(f"[oecd] ✓ Wrote {len(out)} countries · latest period: {as_of}")
    print(f"[oecd] Output: {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

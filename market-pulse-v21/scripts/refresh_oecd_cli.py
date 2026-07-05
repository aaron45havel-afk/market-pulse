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
- OECD's API URL structure has changed multiple times. If the fetch fails
  with a 404 or format error, check
  https://data-explorer.oecd.org/ → CLI dataset → "Developer API"
  for the current URL pattern.
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


def _oecd_sdmx_url() -> str:
    """SDMX-JSON URL for the amplitude-adjusted CLI (LOLITOAA), monthly
    frequency, all countries in our set. Format subject to change — see
    module docstring."""
    countries = "+".join(CODE_MAP.values())
    return (
        "https://sdmx.oecd.org/public/rest/data/"
        "OECD.SDD.STES,DSD_STES@DF_CLI,4.0"
        f"/{countries}.M.LOLITOAA......"
        "?startPeriod=2024-01"
        "&dimensionAtObservation=AllDimensions"
        "&format=jsondata"
    )


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

    obs_dict = data.get("dataSets", [{}])[0].get("observations", {})
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
    url = _oecd_sdmx_url()
    print(f"[oecd] Fetching CLI from OECD SDMX API…")
    print(f"[oecd] URL: {url}")

    try:
        payload = _fetch(url)
    except urllib.error.HTTPError as e:
        print(f"[oecd] HTTP error {e.code}: {e.reason}")
        print("[oecd] The OECD API URL structure has likely changed. See module docstring.")
        return 2
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        print(f"[oecd] Network error: {e}")
        return 3
    except json.JSONDecodeError as e:
        print(f"[oecd] Response wasn't JSON: {e}")
        return 4

    try:
        as_of, series = _extract_series(payload)
    except RuntimeError as e:
        print(f"[oecd] Parse error: {e}")
        return 5

    if not series:
        print("[oecd] Parse returned zero series — nothing to write.")
        return 6

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

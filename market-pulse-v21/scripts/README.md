# Maintenance scripts

## refresh_zillow.py

Refreshes ZIP-level home values + rents in the neighborhood maps.

```bash
python scripts/refresh_zillow.py            # download + write overrides
python scripts/refresh_zillow.py --dry-run  # download + print, don't write
```

Reads every 5-digit ZIP key in `dallas_neighborhoods.py` and
`state_neighborhoods.py`, downloads the latest Zillow Research ZHVI
(home value) and ZORI (rent) CSVs, picks the most recent monthly value
per ZIP, and writes `data/zillow_overrides.json`.

The neighborhood modules apply that file at import time, so cap-rate
scoring uses the fresh numbers without anyone editing source. Reverting
is just deleting the JSON.

Run cadence: quarterly is plenty (cap rates move slowly). Suitable for a
GitHub Action that opens a PR with the JSON diff.

### What gets refreshed

| Field | Source |
|---|---|
| `median_home_value` | Zillow ZHVI all-homes, ZIP level |
| `median_rent_monthly` | Zillow ZORI SFR+condo+MFR, ZIP level |

Everything else (crime index, walk score, restaurant density, % bachelor's,
income, population, lat/lng, tags) stays at the hand-curated snapshot.
Those move slowly and Zillow doesn't publish them.

### When ZIPs are missing

Rural or low-volume ZIPs may not appear in Zillow's public CSVs. The
script logs a list of missing ZIPs at the end of each run. Those keep
their hardcoded values from the snapshot.

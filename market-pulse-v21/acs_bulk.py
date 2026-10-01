"""Census ACS 5-year figures by ZIP (ZCTA), from the Bureau's bulk files — no key.

The Census data API now refuses keyless requests, and its key signup did
not activate for this repo. The same numbers are published as one file per
table, for every geography, at

    www2.census.gov/programs-surveys/acs/summary_file/{year}/table-based-SF/
        data/5YRData/acsdt5y{year}-{table}.dat

which needs no key and is reachable from GitHub's runners (the ZCTA-to-
tract file for the hazard layer comes from the same host). Each file is
pipe-delimited, one row per geography:

    GEO_ID|B25003_E001|B25003_M001|B25003_E002|...
    860Z200US44107|18604|412|9853|398|8751|402

Only ZCTA rows (GEO_ID "860Z200US" + 5 digits) and estimate columns (_E)
are kept, renamed to the API's spelling ("B25003_001E") so callers that
were written against the API read the same keys.

The parser is pure; fetching is a thin loop around it. Used by
scripts/build_national_zips.py (income, education, housing) and
scripts/refresh_rents.py (median gross rent, the ladder's last tier).
"""
from __future__ import annotations

import time
import urllib.error
import urllib.request

URL = ("https://www2.census.gov/programs-surveys/acs/summary_file/{year}/"
       "table-based-SF/data/5YRData/acsdt5y{year}-{table}.dat")
ZCTA_PREFIX = "860Z200US"   # + 5-digit ZCTA
STATE_PREFIX = "0400000US"  # + 2-digit state FIPS
UA = {"User-Agent": "MarketPulse/1.0 (acs bulk; invoice@archfms.com)"}
MIN_ZCTAS = 30_000          # the 2024 files carry 33,772


class AcsUnavailable(Exception):
    """The bulk files could not be read — the caller decides what to keep."""


# The margin-of-error column carries the annotation for its estimate.
# -333333333 means the median sits in an OPEN-ENDED interval: the estimate
# beside it is a placeholder (250001 for "$250,000+", 2499 for "under
# $2,500", 1939 for "built 1939 or earlier"), not a measurement. The other
# MOE codes say no margin can be given (controlled estimate, too few
# samples, not applicable).
MOE_OPEN_INTERVAL = -333333333
MOE_CODES = {
    -333333333: "open-ended interval (top- or bottom-coded median)",
    -555555555: "estimate is controlled; no sampling error",
    -222222222: "too few sample observations for a margin of error",
    -999999999: "too few sample observations for a margin of error",
    -888888888: "margin of error not applicable",
}


def api_name(col: str, keep_moe: bool = False) -> str | None:
    """'B25003_E001' → 'B25003_001E'; margins of error ('_M001') →
    'B25003_001M' when `keep_moe`, else None."""
    table, _, rest = col.partition("_")
    if rest.startswith("E") and rest[1:].isdigit():
        return f"{table}_{rest[1:]}E"
    if keep_moe and rest.startswith("M") and rest[1:].isdigit():
        return f"{table}_{rest[1:]}M"
    return None


def parse_table(lines, wanted: set | None = None, prefix: str = ZCTA_PREFIX,
                keep_moe: bool = False) -> dict:
    """Lines of one table file → {geo: {var: number or None}} for the rows
    whose GEO_ID starts with `prefix` (ZCTAs by default; STATE_PREFIX for
    states), keyed by the rest of the GEO_ID.

    Counts come back as ints; medians such as median age keep their
    decimals. Census writes suppressed or unavailable estimates as negative
    sentinels (-666666666 and kin) or blanks; both become None, never a
    number. `wanted` limits the variables kept (API spelling).

    `keep_moe` also returns each margin of error as '<table>_<line>M'.
    A margin keeps its negative annotation code (see MOE_CODES) instead of
    becoming None, because the code is the only record that the estimate
    beside it is a top- or bottom-code rather than a measured median.
    """
    it = iter(lines)
    header = next(it, "")
    if isinstance(header, bytes):
        header = header.decode("utf-8", "replace")
    cols = header.rstrip("\r\n").split("|")
    if not cols or cols[0] != "GEO_ID":
        raise ValueError(f"not an ACS table file (header starts {header[:40]!r})")
    keep = [(i, api_name(c, keep_moe)) for i, c in enumerate(cols)]
    keep = [(i, n) for i, n in keep
            if n and (wanted is None or n in wanted or n[:-1] + "E" in wanted)]
    out = {}
    for raw in it:
        line = raw.decode("utf-8", "replace") if isinstance(raw, bytes) else raw
        if not line.startswith(prefix):
            continue
        parts = line.rstrip("\r\n").split("|")
        z = parts[0][len(prefix):]
        if not z.isdigit():
            continue
        rec = {}
        for i, name in keep:
            v = parts[i].strip() if i < len(parts) else ""
            try:
                n = float(v)
            except ValueError:
                n = None
            if n is not None and n >= 0:
                rec[name] = int(n) if n.is_integer() else n
            elif n is not None and name.endswith("M") and int(n) in MOE_CODES:
                rec[name] = int(n)
            else:
                rec[name] = None
        out[z] = rec
    return out


def is_open_interval(record: dict, var: str) -> bool:
    """True when `var`'s estimate is a top- or bottom-code placeholder:
    its margin of error carries the open-interval annotation. Needs the
    record to have been parsed with keep_moe=True."""
    return record.get(var[:-1] + "M") == MOE_OPEN_INTERVAL


def _open(url: str, method: str = "GET"):
    return urllib.request.urlopen(urllib.request.Request(url, headers=UA, method=method),
                                  timeout=300)


def latest_year(today_year: int, table: str = "b25003", back: int = 4) -> int:
    """Newest 5-year vintage on the server. The Bureau publishes year Y's
    5-year files around the following December–January, so this looks
    back from last year rather than pinning a vintage that goes stale."""
    for y in range(today_year - 1, today_year - 1 - back, -1):
        try:
            with _open(URL.format(year=y, table=table), "HEAD") as r:
                if r.status == 200:
                    return y
        except urllib.error.HTTPError:
            continue
    raise AcsUnavailable(f"no 5-year ACS table files found for {today_year - back}–{today_year - 1}")


def fetch(tables, year: int, wanted: set | None = None, attempts: int = 3,
          prefix: str = ZCTA_PREFIX, min_rows: int = MIN_ZCTAS,
          keep_moe: bool = False) -> dict:
    """{geo: {var: value}} merged across `tables` for one vintage."""
    merged: dict = {}
    for table in tables:
        last = None
        for i in range(attempts):
            try:
                with _open(URL.format(year=year, table=table)) as r:
                    part = parse_table(r, wanted, prefix, keep_moe)
                break
            except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
                last = e
                time.sleep(5 * (i + 1))
        else:
            raise AcsUnavailable(f"{table} {year}: {last}")
        if len(part) < min_rows:
            raise AcsUnavailable(f"{table} {year}: only {len(part):,} rows (floor {min_rows:,})")
        for z, rec in part.items():
            merged.setdefault(z, {}).update(rec)
    return merged

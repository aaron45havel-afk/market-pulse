"""Refresh the rent ladder in data/zips.db from measured sources only.

REPLACES AN IMPUTATION THAT WAS NOT A RENT. `median_rent_monthly` used
to fall back to `home_value / 17 / 12` for any ZIP outside Zillow's ZORI
file — 17,358 of 25,774 ZIPs. That is the home value over 204, so every
cap rate computed from it was arithmetic on itself: all 17,358 imputed
ZIPs shared five distinct cap rates, each 5.88% by construction.

Three sources, in the precedence rent_ladder.py defines:

  ZORI   Zillow Research public CSV. No key. ~8,400 ZIPs.
         https://files.zillowstatic.com/research/public_csvs/zori/

  SAFMR  HUD Small Area Fair Market Rents, per ZIP, by bedroom. Needs a
  + FMR  free token from huduser.gov/hudapi. County FMR covers the
         non-metro remainder.

  ACS    Census B25064 median gross rent by ZCTA. Uses the same
         CENSUS_API_KEY the state ACS refresh already runs on.

FAIL-SOFT, PER SOURCE. If HUD is down, ZORI and ACS still land and the
run reports reduced coverage. A source that fails NEVER blanks a column
it did not write — the whole point is to stop shipping rents nobody
measured, and wiping good data on a network blip is the same crime in a
different direction.

The parsers below are pure and covered by tests/test_rents_build.py.
The fetch functions do nothing but assemble a URL and hand the body to a
parser, because huduser.gov and api.census.gov are unreachable from the
sandbox this was written in — so anything that can be wrong had to be
provable against a fixture.

Usage:
    python scripts/refresh_rents.py --dry-run     # fetch + report, no write
    python scripts/refresh_rents.py               # write zips.db
    python scripts/refresh_rents.py --skip-hud    # ZORI + ACS only
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import os
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import rent_ladder as RL

log = logging.getLogger("refresh_rents")

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "zips.db"

ZORI_URL = ("https://files.zillowstatic.com/research/public_csvs/zori/"
            "Zip_zori_uc_sfrcondomfr_sm_month.csv")
HUD_BASE = "https://www.huduser.gov/hudapi/public/fmr"
ACS_VINTAGE_DEFAULT = 2023
UA = {"User-Agent": "market-pulse/1"}

# The columns this script owns. Everything else in `zips` is left alone.
RENT_COLUMNS = (
    ("rent_tier", "TEXT"),           # which source answered: zori/safmr/fmr/acs
    ("rent_basis", "TEXT"),          # asking / voucher-floor / occupied-gross
    ("rent_zori", "INTEGER"),
    ("rent_safmr", "INTEGER"),
    ("rent_fmr", "INTEGER"),
    ("rent_acs", "INTEGER"),
    ("rent_br0", "INTEGER"),
    ("rent_br1", "INTEGER"),
    ("rent_br2", "INTEGER"),
    ("rent_br3", "INTEGER"),
    ("rent_br4", "INTEGER"),
    ("rent_bedroom_tier", "TEXT"),
    ("rent_as_of", "TEXT"),
)


# ─── pure parsers ────────────────────────────────────────────────────
def parse_zori_csv(csv_text: str) -> dict[str, int]:
    """{zip: latest monthly ZORI}. Wide CSV, one column per month.

    Takes the LAST column that holds a value for each row rather than
    the last column overall — Zillow pads the trailing months with
    blanks for regions that stopped reporting, and reading the final
    column blindly would drop them entirely.
    """
    if not csv_text:
        return {}
    rows = list(csv.reader(io.StringIO(csv_text)))
    if len(rows) < 2:
        return {}
    header = rows[0]
    try:
        zip_i = header.index("RegionName")
    except ValueError:
        raise SystemExit("ZORI CSV missing RegionName — Zillow changed the schema.")
    # Month columns are the ISO-dated ones at the right-hand end.
    month_idx = [i for i, h in enumerate(header) if _is_month_col(h)]
    if not month_idx:
        raise SystemExit("ZORI CSV has no dated month columns.")

    out: dict[str, int] = {}
    for r in rows[1:]:
        if zip_i >= len(r):
            continue
        z = str(r[zip_i]).strip().zfill(5)
        if len(z) != 5 or not z.isdigit():
            continue
        for i in reversed(month_idx):
            if i < len(r) and (r[i] or "").strip():
                try:
                    v = float(r[i])
                except ValueError:
                    continue
                if RL.is_plausible(v):
                    out[z] = round(v)
                break
    return out


def _is_month_col(h: str) -> bool:
    h = (h or "").strip()
    return len(h) >= 7 and h[:4].isdigit() and h[4] == "-"


def parse_hud_fmr_json(payload) -> dict:
    """HUD /fmr/data/{entity} response → {bedrooms, all_units}.

    HUD has published this under two shapes over the years — a flat
    `basicdata` object and a list of them for multi-county metros. Both
    are handled, and anything else raises rather than returning zeros,
    because a silent {} here would look exactly like "this county has no
    FMR" and quietly drop a whole state.
    """
    data = (payload or {}).get("data") if isinstance(payload, dict) else None
    if not isinstance(data, dict):
        raise ValueError("HUD response has no 'data' object")
    basic = data.get("basicdata")
    if basic is None:
        raise ValueError("HUD response has no 'basicdata'")
    if isinstance(basic, list):
        basic = basic[0] if basic else {}
    if not isinstance(basic, dict):
        raise ValueError("HUD 'basicdata' is neither an object nor a list")
    return {"bedrooms": _hud_bedrooms(basic), "year": data.get("year")}


def parse_hud_safmr_json(payload) -> dict[str, dict]:
    """HUD SAFMR response → {zip: {bedrooms}}.

    The SAFMR payload nests a list of ZIP records under
    data.basicdata, each carrying its own zip_code.
    """
    data = (payload or {}).get("data") if isinstance(payload, dict) else None
    if not isinstance(data, dict):
        raise ValueError("HUD SAFMR response has no 'data' object")
    basic = data.get("basicdata")
    if isinstance(basic, dict):
        basic = [basic]
    if not isinstance(basic, list):
        raise ValueError("HUD SAFMR 'basicdata' is not a list")
    out: dict[str, dict] = {}
    for rec in basic:
        if not isinstance(rec, dict):
            continue
        z = str(rec.get("zip_code") or "").strip().zfill(5)
        if len(z) != 5 or not z.isdigit():
            continue
        beds = _hud_bedrooms(rec)
        if beds:
            out[z] = {"bedrooms": beds, "year": data.get("year")}
    return out


_HUD_BED_KEYS = {
    "0": ("Efficiency", "efficiency", "Studio", "studio", "fmr_0", "0"),
    "1": ("One-Bedroom", "one-bedroom", "OneBedroom", "fmr_1", "1"),
    "2": ("Two-Bedroom", "two-bedroom", "TwoBedroom", "fmr_2", "2"),
    "3": ("Three-Bedroom", "three-bedroom", "ThreeBedroom", "fmr_3", "3"),
    "4": ("Four-Bedroom", "four-bedroom", "FourBedroom", "fmr_4", "4"),
}


def _hud_bedrooms(rec: dict) -> dict:
    """Pull the 0–4 bedroom rents out of a HUD record.

    HUD's key casing has changed between API versions, so several
    spellings are accepted per bedroom. An unrecognised record yields {}
    and the caller falls through a tier rather than storing nothing as
    zero.
    """
    out = {}
    for bed, keys in _HUD_BED_KEYS.items():
        for k in keys:
            if k in rec:
                v = rec[k]
                if RL.is_plausible(v):
                    out[bed] = round(float(v))
                break
    return out


def parse_hud_safmr_csv(csv_text: str) -> dict[str, dict]:
    """HUD's published SAFMR csv → {zip: {bedrooms}}.

    A second way in, for when the API shape surprises us or the token is
    missing: HUD posts the same numbers as a yearly CSV. Column names
    carry the fiscal year (`SAFMR 1BR`, `fy2026_safmr_1`, …) so they are
    matched by shape rather than by an exact string that changes every
    October.
    """
    if not csv_text:
        return {}
    reader = csv.DictReader(io.StringIO(csv_text))
    if not reader.fieldnames:
        return {}
    zip_col = next((c for c in reader.fieldnames
                    if "zip" in c.lower().replace("_", "")), None)
    if not zip_col:
        raise SystemExit("SAFMR CSV has no ZIP column.")

    bed_cols: dict[str, str] = {}
    for c in reader.fieldnames:
        low = c.lower()
        if "safmr" not in low and "fmr" not in low:
            continue
        if "pct" in low or "percent" in low:
            continue
        for bed, token in (("0", "0"), ("1", "1"), ("2", "2"),
                           ("3", "3"), ("4", "4")):
            if (f"{token}br" in low.replace(" ", "")
                    or low.rstrip().endswith(f"_{token}")
                    or low.rstrip().endswith(f" {token}")):
                bed_cols.setdefault(bed, c)
                break

    out: dict[str, dict] = {}
    for row in reader:
        z = str(row.get(zip_col) or "").strip().strip('="\'').split(".")[0].zfill(5)
        if len(z) != 5 or not z.isdigit():
            continue
        beds = {}
        for bed, col in bed_cols.items():
            v = str(row.get(col) or "").replace("$", "").replace(",", "").strip()
            if RL.is_plausible(v):
                beds[bed] = round(float(v))
        if beds:
            out[z] = {"bedrooms": beds, "year": None}
    return out


def parse_acs_zcta(rows) -> dict[str, int]:
    """Census ACS response → {zcta: median gross rent}.

    Census encodes "no data" as large negative sentinels
    (-666666666 and friends). Those become an ABSENCE, never a rent —
    passed through they would read as a negative rent, and clamped to
    zero they would read as free housing.
    """
    if not isinstance(rows, list) or len(rows) < 2:
        return {}
    header = rows[0]
    try:
        val_i = header.index("B25064_001E")
    except ValueError:
        raise SystemExit("ACS response missing B25064_001E — schema changed.")
    zcta_i = next((i for i, h in enumerate(header)
                   if "zip code tabulation area" in h.lower() or h == "zip"), None)
    if zcta_i is None:
        raise SystemExit("ACS response has no ZCTA column.")

    out: dict[str, int] = {}
    for r in rows[1:]:
        if val_i >= len(r) or zcta_i >= len(r):
            continue
        z = str(r[zcta_i]).strip().zfill(5)
        if len(z) != 5 or not z.isdigit():
            continue
        try:
            v = float(r[val_i])
        except (TypeError, ValueError):
            continue
        if v < 0:                      # Census null sentinel
            continue
        if RL.is_plausible(v):
            out[z] = round(v)
    return out


# ─── thin fetch layer ────────────────────────────────────────────────
def _get(url: str, timeout: int = 90, headers: dict | None = None) -> str:
    req = urllib.request.Request(url, headers={**UA, **(headers or {})})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", errors="replace")


def fetch_zori() -> dict[str, int]:
    log.info("Fetching ZORI …")
    return parse_zori_csv(_get(ZORI_URL))


def fetch_acs(vintage: int) -> dict[str, int]:
    key = os.environ.get("CENSUS_API_KEY", "").strip()
    if not key:
        log.warning("CENSUS_API_KEY unset — skipping the ACS tier.")
        return {}
    url = (f"https://api.census.gov/data/{vintage}/acs/acs5"
           f"?get=B25064_001E&for=zip%20code%20tabulation%20area:*&key={key}")
    log.info("Fetching ACS %d B25064 for all ZCTAs …", vintage)
    return parse_acs_zcta(json.loads(_get(url)))


def fetch_hud(states: list[str], token: str, pause: float = 0.4) -> tuple[dict, dict]:
    """(safmr_by_zip, fmr_by_county). One request per county, rate-limited.

    Returns whatever it managed to collect. A failure on one county logs
    and continues — losing Ohio should not cost the other forty-nine.
    """
    safmr: dict[str, dict] = {}
    fmr: dict[str, dict] = {}
    hdr = {"Authorization": f"Bearer {token}"}
    for st in states:
        try:
            counties = json.loads(_get(f"{HUD_BASE}/listCounties/{st}", headers=hdr))
        except Exception as e:
            log.warning("HUD county list failed for %s: %s", st, e)
            continue
        if not isinstance(counties, list):
            continue
        for c in counties:
            fips = str((c or {}).get("fips_code") or "").strip()
            if not fips:
                continue
            try:
                payload = json.loads(_get(f"{HUD_BASE}/data/{fips}", headers=hdr))
            except Exception as e:
                log.debug("HUD data failed for %s: %s", fips, e)
                continue
            # SAFMR ZIP records ride in the same payload for metro areas.
            try:
                z = parse_hud_safmr_json(payload)
                safmr.update(z)
            except ValueError:
                pass
            try:
                fmr[fips[:5]] = parse_hud_fmr_json(payload)
            except ValueError as e:
                log.debug("HUD county parse failed for %s: %s", fips, e)
            time.sleep(pause)
        log.info("HUD %s: %d SAFMR ZIPs, %d counties so far", st, len(safmr), len(fmr))
    return safmr, fmr


# ─── DB ──────────────────────────────────────────────────────────────
def ensure_columns(conn: sqlite3.Connection) -> None:
    have = {r[1] for r in conn.execute("PRAGMA table_info(zips)")}
    for name, typ in RENT_COLUMNS:
        if name not in have:
            conn.execute(f"ALTER TABLE zips ADD COLUMN {name} {typ}")
    conn.commit()


def county_fips_map(conn: sqlite3.Connection) -> dict[str, str]:
    """{zip: county_fips} — only if the DB carries one. Empty otherwise.

    The county FMR tier needs a ZIP-to-county mapping. `zips` stores a
    county NAME, not a FIPS code, so without a real crosswalk this tier
    stays empty rather than guessing from a name that is ambiguous
    across states.
    """
    cols = {r[1] for r in conn.execute("PRAGMA table_info(zips)")}
    if "county_fips" not in cols:
        return {}
    return {r[0]: r[1] for r in
            conn.execute("SELECT zip, county_fips FROM zips WHERE county_fips IS NOT NULL")}


def carry_stored(conn: sqlite3.Connection) -> tuple[dict, dict, dict]:
    """Stored per-tier values, for sources that did NOT fetch this run.

    THIS IS THE DIFFERENCE BETWEEN A PARTIAL RUN AND A DESTRUCTIVE ONE.
    A run with --skip-hud recomputes every row from whatever it did
    fetch, so a ZIP whose winning tier was SAFMR resolves to nothing and
    its rent, tier and cap rate are all blanked — by a run that was
    never asked to touch HUD at all. Carrying the stored columns for
    unfetched sources means a partial run can only improve a row or
    leave it alone.

    A source that DID fetch is authoritative, including its silences: a
    ZIP that ZORI dropped this month should lose its ZORI value. So the
    caller carries only for sources it did not successfully reach.
    """
    z = {r[0]: r[1] for r in conn.execute(
        "SELECT zip, rent_zori FROM zips WHERE rent_zori IS NOT NULL")}
    a = {r[0]: r[1] for r in conn.execute(
        "SELECT zip, rent_acs FROM zips WHERE rent_acs IS NOT NULL")}
    s: dict[str, dict] = {}
    for zip_, br0, br1, br2, br3, br4, two in conn.execute(
            "SELECT zip, rent_br0, rent_br1, rent_br2, rent_br3, rent_br4, "
            "rent_safmr FROM zips WHERE rent_safmr IS NOT NULL "
            "OR rent_br2 IS NOT NULL"):
        beds = {b: v for b, v in zip(RL.BEDROOMS, (br0, br1, br2, br3, br4))
                if v is not None}
        if beds or two is not None:
            s[zip_] = {"bedrooms": beds or {"2": two}, "year": None}
    return z, s, a


def apply(conn: sqlite3.Connection, zori, safmr, fmr_by_county, acs,
          fips_of, as_of: str, dry_run: bool) -> dict:
    """Resolve every ZIP through the ladder and write the result.

    The persona composites are recomputed too, because cap_rate is one
    of their seven inputs and the rent underneath it just changed. That
    also re-syncs a pre-existing drift: 401 of a 3,000-row sample had
    stored composites that no longer matched a recompute from their own
    columns, because crime_index was later overwritten by the crime
    refresh without anything rescoring the board.
    """
    have = {r[1] for r in conn.execute("PRAGMA table_info(zips)")}
    scoring = {"crime_index", "pct_bachelors", "median_household_income",
               "walk_score", "restaurant_score", "composite_balanced",
               "composite_investor", "composite_lifestyle", "composite_score"}
    rescore = scoring <= have
    if rescore:
        import dallas_neighborhoods as DN
        rows = list(conn.execute(
            "SELECT zip, median_home_value, crime_index, pct_bachelors, "
            "median_household_income, walk_score, restaurant_score "
            "FROM zips ORDER BY zip"))
    else:
        log.warning("Scoring columns absent — writing rents without rescoring.")
        rows = [(r[0], r[1], None, None, None, None, None) for r in conn.execute(
            "SELECT zip, median_home_value FROM zips ORDER BY zip")]

    tiers = []
    writes = []
    rescores = []
    for z, hv, crime, bach, inc, walk, rest in rows:
        s = safmr.get(z) or {}
        f = fmr_by_county.get(fips_of.get(z, "")) or {}
        s_beds, f_beds = s.get("bedrooms"), f.get("bedrooms")
        res = RL.resolve(
            zori=zori.get(z),
            # The headline HUD figure is the 2-bedroom, which is the
            # standard reference unit in every HUD table.
            safmr=(s_beds or {}).get("2"),
            fmr=(f_beds or {}).get("2"),
            acs=acs.get(z),
            safmr_bedrooms=s_beds, fmr_bedrooms=f_beds, as_of=as_of)
        tiers.append(res["tier"])
        beds = res["by_bedroom"] or {}
        writes.append((
            res["rent"], res["tier"], res["basis"],
            zori.get(z), (s_beds or {}).get("2"), (f_beds or {}).get("2"), acs.get(z),
            beds.get("0"), beds.get("1"), beds.get("2"), beds.get("3"), beds.get("4"),
            res["by_bedroom_tier"],
            RL.cap_rate_pct(res["rent"], hv),
            as_of, z,
        ))
        if rescore and hv:
            m = DN.compute_zip_metrics({
                "median_home_value": hv, "median_rent_monthly": res["rent"],
                "crime_index": crime, "pct_bachelors": bach,
                "median_household_income": inc,
                "walk_score": walk or 0, "restaurant_score": rest or 0})
            p = m["composite_by_persona"]
            rescores.append((p["balanced"], p["investor"], p["lifestyle"],
                             m["composite_score"], z))
    if not dry_run:
        conn.executemany("""
            UPDATE zips SET median_rent_monthly=?, rent_tier=?, rent_basis=?,
                rent_zori=?, rent_safmr=?, rent_fmr=?, rent_acs=?,
                rent_br0=?, rent_br1=?, rent_br2=?, rent_br3=?, rent_br4=?,
                rent_bedroom_tier=?, cap_rate_pct=?, rent_as_of=?
            WHERE zip=?
        """, writes)
        # rent_source is the old two-value column ('zori'/'imputed'). Keep
        # it in step with rent_tier so any reader not yet migrated stops
        # seeing 'imputed' on rows that now carry a real measurement.
        conn.execute("UPDATE zips SET rent_source = rent_tier")
        if rescores:
            conn.executemany(
                "UPDATE zips SET composite_balanced=?, composite_investor=?, "
                "composite_lifestyle=?, composite_score=? WHERE zip=?", rescores)
            log.info("  rescored %d ZIPs", len(rescores))
        conn.commit()
    return RL.coverage([{"tier": t} for t in tiers])


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--dry-run", action="store_true",
                    help="fetch and report coverage without writing")
    ap.add_argument("--skip-hud", action="store_true")
    ap.add_argument("--skip-acs", action="store_true")
    ap.add_argument("--acs-vintage", type=int, default=ACS_VINTAGE_DEFAULT)
    ap.add_argument("--safmr-csv", default="",
                    help="path or URL to HUD's published SAFMR CSV, used "
                         "instead of the API")
    ap.add_argument("--states", default="",
                    help="comma-separated state codes to limit the HUD pull")
    args = ap.parse_args(argv)

    if not DB_PATH.exists():
        log.error("No %s — run build_national_zips.py first.", DB_PATH)
        return 1
    conn = sqlite3.connect(DB_PATH)
    ensure_columns(conn)

    from datetime import date
    as_of = date.today().isoformat()

    # Each source is independent. One failure costs its own tier and
    # nothing else — a network blip must not blank a column it did not
    # write.
    got_zori = got_hud = got_acs = False
    zori: dict = {}
    try:
        zori = fetch_zori()
        got_zori = True
        log.info("  ZORI: %d ZIPs", len(zori))
    except Exception as e:
        log.warning("  ZORI FAILED (%s)", e)

    safmr: dict = {}
    fmr: dict = {}
    if not args.skip_hud:
        if args.safmr_csv:
            try:
                text = (Path(args.safmr_csv).read_text()
                        if not args.safmr_csv.startswith("http")
                        else _get(args.safmr_csv))
                safmr = parse_hud_safmr_csv(text)
                got_hud = True
                log.info("  SAFMR (csv): %d ZIPs", len(safmr))
            except Exception as e:
                log.warning("  SAFMR CSV FAILED (%s)", e)
        else:
            token = os.environ.get("HUD_API_TOKEN", "").strip()
            if not token:
                log.warning("  HUD_API_TOKEN unset — skipping the HUD tiers. "
                            "Get one free at huduser.gov/hudapi.")
            else:
                states = ([s.strip().upper() for s in args.states.split(",") if s.strip()]
                          or sorted({r[0] for r in conn.execute(
                              "SELECT DISTINCT state FROM zips WHERE state IS NOT NULL")}))
                try:
                    safmr, fmr = fetch_hud(states, token)
                    got_hud = True
                    log.info("  SAFMR: %d ZIPs, FMR: %d counties", len(safmr), len(fmr))
                except Exception as e:
                    log.warning("  HUD FAILED (%s)", e)

    acs: dict = {}
    if not args.skip_acs:
        try:
            acs = fetch_acs(args.acs_vintage)
            got_acs = bool(acs)
            log.info("  ACS: %d ZCTAs", len(acs))
        except Exception as e:
            log.warning("  ACS FAILED (%s)", e)

    if not (got_zori or got_hud or got_acs):
        log.error("Every source failed — refusing to write. The stored "
                  "rents stay exactly as they are.")
        return 1

    # CARRY THE STORED VALUES FOR ANY SOURCE THAT DID NOT REACH.
    # apply() recomputes every row from what it is handed, so without
    # this a --skip-hud run would blank every ZIP whose winning tier was
    # SAFMR — destroying good data on a run that was never asked to
    # touch HUD. A source that DID fetch stays authoritative, silences
    # included, so a ZIP that ZORI genuinely dropped still loses it.
    c_zori, c_safmr, c_acs = carry_stored(conn)
    if not got_zori:
        zori = c_zori
        log.info("  ZORI not fetched — carried %d stored values", len(zori))
    if not got_hud:
        safmr = c_safmr
        log.info("  HUD not fetched — carried %d stored ZIP records", len(safmr))
    if not got_acs:
        acs = c_acs
        log.info("  ACS not fetched — carried %d stored values", len(acs))

    cov = apply(conn, zori, safmr, fmr, acs, county_fips_map(conn), as_of,
                args.dry_run)
    conn.close()

    print(f"\n{'DRY RUN — nothing written' if args.dry_run else f'Wrote {DB_PATH}'}")
    print(f"  ZIPs                 {cov['total']:>7,}")
    for k in RL.TIER_ORDER:
        print(f"  {k:<20} {cov['by_tier'][k]:>7,}")
    print(f"  {'no measured rent':<20} {cov['none']:>7,}")
    print(f"  measured             {cov['real_pct']:>6}%   "
          f"(was 33% — the rest was home_value / 17 / 12)")
    return 0


if __name__ == "__main__":
    sys.exit(main())

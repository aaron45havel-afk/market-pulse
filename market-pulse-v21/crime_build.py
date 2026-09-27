"""Which FBI city figures are trustworthy enough to call a place safe.

Pure: no network, no database, no clock. scripts/fetch_fbi_crime.py pulls
every city police agency's monthly counts; scripts/build_crime.py feeds
them through here and writes data/headroom/crime.json, which safety.py
reads. Tested in tests/test_crime_build.py.

THE FAILURE THAT MATTERS is a false "safe". A department that reported
four months, or switched reporting systems and lost half its incidents,
produces a rate that looks like the safest town in the state. Every rule
below exists to turn that case into UNKNOWN — which the board already
treats as not safe — rather than into a green label.

  COMPLETE YEARS ONLY. A year counts only when the agency reported all
  twelve months of BOTH violent and property crime. Nothing is scaled up
  from a partial year.

  SMALL TOWNS POOL. Below POOL_BELOW residents one incident moves the
  rate by more than a tier (a village of 900: one assault is 111 per
  100k), so small agencies pool every complete year in the window.
  Larger ones use their latest complete year.

  A COLLAPSE IS A REPORTING CHANGE UNTIL PROVEN OTHERWISE. A latest
  year with under DROP_RATIO of the agency's own prior average — or no
  violent crime at all, for years, in a town of ZERO_POP or more — is
  marked suspect, never safe.

  NAMES MATCH ONLY WHERE THEY ARE UNAMBIGUOUS AND NEARBY. An agency is
  tied to a ZIP city by state and name (spelling-normalised) and only
  if the agency sits within MAX_KM of that city's ZIPs.
"""
from __future__ import annotations

import math
import re
import unicodedata

POOL_BELOW = 10_000          # residents; smaller agencies pool complete years
DROP_RATIO = 0.40            # latest year under 40% of the prior average → suspect
DROP_MIN_PRIOR = 10          # ...when the prior average is at least this many offenses
ZERO_POP = 5_000             # zero violent crime across every complete year → suspect
MAX_KM = 40.0                # agency to the centroid of its city's ZIPs


# ─── names ───────────────────────────────────────────────────────────
def norm_place(name: str) -> str:
    """Spelling-only: accents, case, punctuation, spacing, Saint/St.,
    Mount/Mt., Fort/Ft. The words themselves are kept, so "Springfield"
    and "Springfield Township" stay apart."""
    n = unicodedata.normalize("NFKD", name or "")
    n = "".join(ch for ch in n if not unicodedata.combining(ch)).casefold()
    for ch in ".'’":
        n = n.replace(ch, "")
    words = n.replace("-", " ").split()
    alias = {"saint": "st", "sainte": "ste", "mount": "mt", "fort": "ft"}
    return "".join(alias.get(w, w) for w in words)


_TAIL = re.compile(r",\s*[^,]*\bcounty\b.*$", re.I)
_SUFFIX = re.compile(
    r"\s+(?:(?:city|town|village|borough|township)\s+)?"
    r"(?:police\s+(?:department|dept\.?|division)|division\s+of\s+police|"
    r"department\s+of\s+public\s+safety|public\s+safety\s+department|"
    r"marshal'?s?\s+office|police)\s*$", re.I)
_PREFIX = re.compile(r"^(?:city|town|village|borough|township)\s+of\s+", re.I)


def agency_place(agency_name: str) -> tuple[str | None, bool]:
    """'Akron Police Department' → ('Akron', False).
    'Colerain Township Police Department, Hamilton County' → ('Colerain', True).

    Returns (place, is_township). None when the name doesn't reduce to a
    place — better no match than a wrong one.
    """
    s = _TAIL.sub("", (agency_name or "").strip())
    township = bool(re.search(r"\btownship\b", s, re.I))
    before = s
    s = _SUFFIX.sub("", s)
    if s == before:
        return None, township
    s = _PREFIX.sub("", s).strip()
    s = re.sub(r"\s+(?:township|borough|village|town|city)$", "", s, flags=re.I).strip()
    return (s or None), township


def km(lat1, lng1, lat2, lng2) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lng2 - lng1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * 6371.0088 * math.asin(math.sqrt(a))


# ─── rates ───────────────────────────────────────────────────────────
def trusted_rate(agency: dict) -> dict:
    """Per-100k rates for one agency, and whether to trust them.

    Returns {"violent", "property", "basis", "years", "population",
    "suspect"} — "suspect" is None or the reason. violent is None when the
    agency has no complete year at all.
    """
    v, p = agency.get("v") or {}, agency.get("p") or {}
    complete = sorted(
        y for y in v
        if (v[y] or {}).get("m") == 12 and (p.get(y) or {}).get("m") == 12
        and (v[y].get("pop") or 0) > 0)
    if not complete:
        return {"violent": None, "property": None, "basis": None, "years": [],
                "population": None,
                "suspect": "no complete year of FBI reporting in the window"}
    latest = complete[-1]
    pop = v[latest]["pop"]
    use = complete if pop < POOL_BELOW else [latest]
    pop_sum = sum(v[y]["pop"] for y in use)
    violent = sum(v[y]["n"] for y in use) / pop_sum * 100_000
    prop = sum(p[y]["n"] for y in use) / pop_sum * 100_000

    suspect = None
    prior = [y for y in complete if y != latest]
    for key, label in (("v", "violent"), ("p", "property")):
        series = v if key == "v" else p
        if prior:
            avg = sum(series[y]["n"] for y in prior) / len(prior)
            now = series[latest]["n"]
            if avg >= DROP_MIN_PRIOR and now < DROP_RATIO * avg:
                suspect = (f"{label} offenses fell from an average of {avg:.0f} to {now} "
                           f"in {latest} — more often a reporting change than a real drop")
                break
    if suspect is None and pop >= ZERO_POP and all(v[y]["n"] == 0 for y in complete):
        suspect = (f"no violent offenses reported in {len(complete)} complete year(s) for "
                   f"a population of {pop:,} — a reporting gap, not a record")
    return {"violent": round(violent, 1), "property": round(prop, 1),
            "basis": "latest" if len(use) == 1 else "pooled",
            "years": use, "population": pop, "suspect": suspect}


# ─── matching + merging ──────────────────────────────────────────────
def match_agencies(agencies: list, cities: dict) -> tuple[dict, dict]:
    """Tie agencies to ZIP cities.

    cities: {(state, norm_place): {"key": "City, ST", "lat", "lng"}} from zips.db.
    Returns ({"City, ST": agency}, report). A city name claimed by two
    non-township agencies (or two townships and no city) is ambiguous and
    gets none; a city agency beats a township agency of the same name.
    """
    claims: dict = {}
    report = {"unnamed": 0, "no_city": 0, "too_far": 0, "ambiguous": 0}
    for a in agencies:
        place, township = agency_place(a.get("name", ""))
        if not place:
            report["unnamed"] += 1
            continue
        city = cities.get((a.get("state"), norm_place(place)))
        if not city:
            report["no_city"] += 1
            continue
        if a.get("lat") is None or a.get("lng") is None or city.get("lat") is None:
            report["too_far"] += 1
            continue
        if km(a["lat"], a["lng"], city["lat"], city["lng"]) > MAX_KM:
            report["too_far"] += 1
            continue
        claims.setdefault(city["key"], []).append((township, a))
    out = {}
    for key, cands in claims.items():
        cities_ = [a for t, a in cands if not t]
        towns = [a for t, a in cands if t]
        pick = cities_ if cities_ else towns
        if len(pick) == 1:
            out[key] = pick[0]
        else:
            report["ambiguous"] += 1
    return out, report


def entry_for(agency: dict, rate: dict, as_of_years: list) -> dict:
    """A crime.json table entry from one agency's trusted rate."""
    yrs = rate["years"]
    year = (str(yrs[0]) if len(yrs) == 1 else f"{yrs[0]}–{yrs[-1]}") if yrs else None
    src = f"FBI Crime Data Explorer, {agency.get('name')} ({agency.get('ori')})"
    rec = {"violent_per_100k": rate["violent"], "property_per_100k": rate["property"],
           "year": year, "source": src, "ori": agency.get("ori"),
           "population": rate["population"], "confidence": "high",
           "basis": rate["basis"]}
    if rate["violent"] is None:
        rec["confidence"] = None
        rec["note"] = (f"The FBI has no complete year of reporting from this department "
                       f"for {as_of_years[0]}–{as_of_years[-1]}.")
    elif rate["suspect"]:
        rec["confidence"] = "suspect"
        rec["note"] = f"Not used: {rate['suspect']}."
    elif rate["basis"] == "pooled":
        rec["note"] = (f"Small town (population {rate['population']:,}): the rate pools "
                       f"{year} so one incident can't swing it a tier.")
    return rec


def merge(existing: dict, fbi: dict) -> tuple[dict, dict]:
    """Existing hand-researched table + FBI entries → new table.

    - A researcher's "suspect" stays suspect; the FBI figure is attached
      under "fbi" for review, never promoted to a label.
    - Otherwise an FBI figure replaces the researched one (a primary source
      over aggregators); the old note is kept as "prior_note".
    - A researched city the FBI didn't match is kept as it was.
    """
    table = {}
    counts = {"fbi": 0, "kept_suspect": 0, "replaced": 0, "kept_research": 0}
    for key, rec in existing.items():
        if key not in fbi:
            table[key] = rec
            counts["kept_research"] += 1
    for key, rec in fbi.items():
        old = existing.get(key)
        if old and old.get("confidence") == "suspect":
            table[key] = dict(old, fbi=rec)
            counts["kept_suspect"] += 1
            continue
        if old:
            rec = dict(rec, prior_note=old.get("note"))
            counts["replaced"] += 1
        table[key] = rec
        counts["fbi"] += 1
    return table, counts

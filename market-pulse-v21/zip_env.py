"""Weather and natural-hazard figures per ZIP — the pure half.

Two free, national, measured sources, each joined to a ZIP a different way:

  * WEATHER from NOAA's 1991–2020 U.S. Climate Normals — thirty-year averages
    measured at weather stations. A ZIP takes the figures of its NEAREST
    station that actually reports them, within a distance cap.

  * HAZARDS from FEMA's National Risk Index — EXPECTED ANNUAL BUILDING LOSS
    per census tract, divided by the tract's building value: dollars of
    damage a year per dollar of building. A ZIP apportions each tract it
    overlaps by land share, using the Census ZCTA-to-tract relationship file.

    Not FEMA's headline risk RATINGS. Those multiply expected loss by the
    local population's social vulnerability, so the same flood rates riskier
    in a poorer tract — the income correlation that got crime_index thrown
    off this board — and they are relative: a wildfire score of 64 is rated
    "Very Low". A buyer's question is what the hazard costs the building.

The fetch scripts do the downloading; everything that decides a number lives
here, so it can be tested without a network.

────────────────────────────────────────────────────────────────────
THE TWO WAYS THIS CAN LIE, AND WHAT STOPS EACH
────────────────────────────────────────────────────────────────────
NEAREST STATION IS NOT NEAREST USEFUL STATION. Most NOAA stations measure
precipitation only. A ZIP whose closest station is a rain gauge must take its
temperatures from the closest station that HAS temperatures, not come back
empty and not borrow a zero. So each variable group — temperature, snow — is
matched separately, each against only the stations that report it, and each
records its own station and distance.

A FAR STATION IS NOT THE ZIP'S WEATHER. Past MAX_STATION_KM the match is
refused and the ZIP has NO DATA, which the finder never lets pass a filter.
Within it, the distance is kept so the page can show it. Elevation is the
residual error this cannot fix: a mountain ZIP matched to a valley station
40 km away will read warm. The station's elevation is stored so that is
visible rather than hidden.

A PARTLY-SCORED ZIP IS NOT A SCORED ZIP. A ZIP overlapping three tracts, only
one of which FEMA could score, would otherwise take that one tract's loss
rate as the whole ZIP's. Below MIN_SCORED_SHARE of its land in scored tracts,
the ZIP has no data.
"""
from __future__ import annotations

import bisect
import math

EARTH_KM = 6371.0088

# ~25 miles. Beyond this a station's normals describe somewhere else. Chosen
# so that almost every metro and suburban ZIP matches while a ZIP in a sparse
# mountain or desert county honestly comes back without a figure.
MAX_STATION_KM = 40.0

# A ZIP needs at least this share of its land in tracts FEMA actually scored.
MIN_SCORED_SHARE = 0.5


def haversine_km(lat1, lng1, lat2, lng2) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lng2 - lng1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * EARTH_KM * math.asin(min(1.0, math.sqrt(a)))


def _finite(v) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)


class StationIndex:
    """Nearest-station lookup over stations that report a given set of fields.

    A 1°×1° grid, searched outward ring by ring. Brute force is 25,000 ZIPs
    against ~7,000 temperature stations — 175 million distance calls, minutes
    in pure Python. The grid makes it seconds and returns the same answer:
    a ring is only skipped once every cell in it is provably farther than the
    best match already found.
    """

    def __init__(self, stations: list, required: tuple):
        self.required = tuple(required)
        self.cells: dict = {}
        self.count = 0
        for s in stations:
            if not (_finite(s.get("lat")) and _finite(s.get("lng"))):
                continue
            vals = s.get("values") or {}
            if not all(_finite(vals.get(k)) for k in self.required):
                continue
            key = (math.floor(s["lat"]), math.floor(s["lng"]))
            self.cells.setdefault(key, []).append(s)
            self.count += 1

    def nearest(self, lat, lng, max_km: float = MAX_STATION_KM):
        """(station, km) or (None, None) when nothing qualifies in range."""
        if not (_finite(lat) and _finite(lng)) or not self.cells:
            return None, None
        cy, cx = math.floor(lat), math.floor(lng)
        best, best_km = None, None
        for ring in range(0, 360):
            # Every cell in ring r lies at least (r - 1) whole cells away along
            # one axis. A degree of latitude is ~111 km; a degree of longitude
            # is 111·cos(latitude) and SHRINKS toward the pole, so the bound
            # uses the most poleward latitude the ring reaches — it can only
            # ever be too cautious, never too eager. With a 40 km cap the gap
            # between this and the query latitude's width is a few percent of
            # 40 km and no counterexample was found in testing; the reason to
            # prefer it is that it is provably safe, not that the other failed.
            reach = min(89.9, abs(lat) + ring + 1)
            cell_km = min(111.0, max(1e-6, 111.32 * math.cos(math.radians(reach))))
            floor_km = max(0, ring - 1) * cell_km
            if floor_km > max_km or (best_km is not None and floor_km > best_km):
                break
            for dy in range(-ring, ring + 1):
                for dx in range(-ring, ring + 1):
                    if max(abs(dy), abs(dx)) != ring:
                        continue
                    for s in self.cells.get((cy + dy, cx + dx), ()):
                        d = haversine_km(lat, lng, s["lat"], s["lng"])
                        if d <= max_km and (best_km is None or d < best_km
                                            or (d == best_km and s["id"] < best["id"])):
                            best, best_km = s, d
        return best, (round(best_km, 1) if best_km is not None else None)


# ── NOAA: one station row -> one station ─────────────────────────────
# Column names and flag meanings as observed in the 1991–2020 annual/seasonal
# archive (c20230404). Every station is one row. A variable a station does not
# report is simply ABSENT from its file — there are no sentinel numbers, which
# the probe checked across all 15,616 stations.
NORMALS = {
    "winter_low": "DJF-TMIN-NORMAL",           # avg daily low, Dec–Feb, °F
    "summer_high": "JJA-TMAX-NORMAL",          # avg daily high, Jun–Aug, °F
    "days_90": "ANN-TMAX-AVGNDS-GRTH090",      # days a year reaching 90°F+
    "days_32": "ANN-TMIN-AVGNDS-LSTH032",      # nights a year at/below freezing
    "snow_in": "ANN-SNOW-NORMAL",              # inches a year
}
TEMP_FIELDS = ("winter_low", "summer_high", "days_90")
SNOW_FIELDS = ("snow_in",)

# Completeness flags. S standard, R representative, P provisional (fewer
# years), E ESTIMATED — computed from neighbouring stations rather than
# measured at the site. A ZIP matched to an E station would be reading its
# neighbours' weather at one remove, so E never qualifies; the ZIP falls
# through to the nearest station that measured its own.
ACCEPTED_COMP = frozenset({"S", "R", "P"})


def station_from_normals(row: dict) -> dict | None:
    """A station dict from one CSV row of the annual/seasonal archive.

    A value is kept only if it parses as a finite number AND its completeness
    flag is accepted; otherwise that one value is dropped and the station may
    still serve for the others. Returns None when the row has no usable
    coordinates.
    """
    def num(v):
        try:
            x = float(str(v).strip())
        except (TypeError, ValueError):
            return None
        return x if math.isfinite(x) else None

    lat, lng = num(row.get("LATITUDE")), num(row.get("LONGITUDE"))
    if lat is None or lng is None:
        return None
    values, flags = {}, {}
    for key, col in NORMALS.items():
        if col not in row:
            continue
        v = num(row.get(col))
        flag = (row.get("comp_flag_" + col) or "").strip()
        if v is None or flag not in ACCEPTED_COMP:
            continue
        values[key], flags[key] = v, flag
    return {"id": (row.get("STATION") or "").strip(), "name": (row.get("NAME") or "").strip(),
            "lat": lat, "lng": lng, "elev_m": num(row.get("ELEVATION")),
            "values": values, "flags": flags}


# ── FEMA NRI: one tract -> expected annual building loss ─────────────
# The probe of the December 2025 release showed exactly three ways a
# hazard's expected annual building loss (…_EALB) can be empty or zero, and
# they mean different things:
#
#   "Not Applicable"             EALB null  — the hazard cannot occur there
#                                             (coastal flooding in Ohio). ZERO.
#   "No Expected Annual Losses"  EALB 0     — it can, FEMA expects no loss. ZERO.
#   "Insufficient Data"          EALB null  — FEMA could not say. MISSING.
#
# Reading every null as missing would give every inland ZIP no flood data;
# reading every null as zero would call unscored places safe. The rating
# text is what tells them apart, so it is required alongside the number.
NOT_APPLICABLE = "Not Applicable"
INSUFFICIENT = "Insufficient Data"
KNOWN_RATINGS = frozenset({
    NOT_APPLICABLE, INSUFFICIENT, "No Expected Annual Losses",
    "Very Low", "Relatively Low", "Relatively Moderate", "Relatively High", "Very High"})

# Hazard groups a buyer filters on, as FEMA hazard codes whose building
# losses ADD. Heat, cold and winter weather are left out on purpose: their
# building losses are near zero nationally (the probe's 99th percentile is
# under $2.10 per $100k a year) and their real cost is comfort, which the
# NOAA filters already cover.
HAZARD_GROUPS = {
    "flood": ("IFLD", "CFLD"),     # inland + coastal
    "wildfire": ("WFIR",),
    "wind": ("HRCN", "TRND"),      # hurricane + tornado
    "quake": ("ERQK",),
}


def tract_loss(rec: dict, codes: tuple):
    """Expected annual building loss ($/yr) for one tract over some hazards.

    None if ANY component is missing — a flood figure built from inland
    losses alone, with coastal unknown, would understate exactly the tracts
    where coastal flooding is the risk. Raises on a rating this code has
    never seen, because guessing what an unfamiliar rating means is how a
    missing value turns into a zero.
    """
    total = 0.0
    for code in codes:
        rating = rec.get(f"{code}_EALR")
        v = rec.get(f"{code}_EALB")
        if rating is not None and rating not in KNOWN_RATINGS:
            raise ValueError(f"unknown NRI rating {rating!r} for {code}")
        if _finite(v):
            total += float(v)
        elif rating == NOT_APPLICABLE:
            continue
        else:
            return None
    return total


def zcta_loss_rate(parts: list, tracts: dict, min_share: float = MIN_SCORED_SHARE):
    """Expected annual building loss as a share of building value, for a ZIP.

    parts:  [(tract_id, land_in_zip, tract_land_total), ...]
    tracts: {tract_id: {"build": building value $, "loss": $/yr or None}}

    Each tract contributes the share of its land that lies in the ZIP — of
    its building value AND of its expected loss — so the result is the loss
    rate of the building stock the ZIP actually contains, assuming buildings
    are spread evenly across each tract's land. That assumption is the
    residual error; it is far smaller than using the tract's rating label,
    which folds in the local population's social vulnerability.

    Returns (rate, covered_share). rate None when less than `min_share` of
    the ZIP's land lies in tracts with a known loss and non-zero building
    value.
    """
    land_total = covered = 0.0
    loss = build = 0.0
    for tract, land_in, land_tract in parts:
        if not (_finite(land_in) and land_in > 0):
            continue
        land_total += land_in
        t = tracts.get(tract)
        if not t or not (_finite(land_tract) and land_tract > 0):
            continue
        b, l = t.get("build"), t.get("loss")
        if not (_finite(b) and b > 0) or not _finite(l):
            continue
        w = min(1.0, land_in / land_tract)
        covered += land_in
        build += w * b
        loss += w * l
    share = covered / land_total if land_total else 0.0
    if share < min_share or build <= 0:
        return None, round(share, 3)
    return loss / build, round(share, 3)


def national_percentile(values: dict) -> dict:
    """{key: value} -> {key: percentile 0–100}, ties sharing the midpoint.

    Higher value, higher percentile. None stays None and does not move the
    others. Used to say "this ZIP is in the worst 10% nationally for flood
    loss", where the national set is every ZIP the board could show.
    """
    known = sorted(v for v in values.values() if _finite(v))
    n = len(known)
    out = {}
    for k, v in values.items():
        if not _finite(v):
            out[k] = None
        elif n == 1:
            out[k] = 50.0
        else:
            lo, hi = bisect.bisect_left(known, v), bisect.bisect_right(known, v) - 1
            out[k] = round((lo + hi) / 2 / (n - 1) * 100, 1)
    return out

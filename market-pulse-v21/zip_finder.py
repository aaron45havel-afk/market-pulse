"""Find ZIPs by what YOU want, not by one fixed formula.

/multifamily used to run a single weighted blend and show its top picks. The
blend was reasonable, but it answered "which ZIPs does this formula like",
and the question a buyer brings is the other way round: "here is what I want —
which ZIPs have it?" This module holds the two halves of that:

  * FILTERS are hard yes/no. "Household income over $75k." A ZIP passes or it
    is gone, and the page says how many each filter removed.

  * PRIORITIES are soft. Among the ZIPs that survive, rank by what matters to
    you — cash flow, growth, neighborhood — each weighted 0 to 3.

────────────────────────────────────────────────────────────────────
MISSING DATA NEVER PASSES A FILTER
────────────────────────────────────────────────────────────────────
Every filter has three outcomes, not two: pass, fail, and NO DATA. A ZIP with
no figure for an active filter is removed and counted separately, so the page
can say "12 dropped for having no figure" rather than folding them in with
"12 dropped for being too low". The first is a gap in our data; the second is
a finding about the place. Letting the first pass would be this repo's house
failure exactly — a value nobody measured rendered as a confident answer.

The same rule already governs the FBI safety gate (safety.py), and this is
that rule made general, so each filter added later — weather, age, hazards —
inherits it rather than having to remember it.

────────────────────────────────────────────────────────────────────
WHAT IS DELIBERATELY NOT A FILTER HERE
────────────────────────────────────────────────────────────────────
zips.db carries three columns whose names promise measurements they are not:
`walk_score` is a curve on population density, `restaurant_score` is a curve
on walk_score, and `crime_index` is density + income + education. Offering
walkability, restaurants and density as three filters would be filtering on
density three times. Density is offered once, under its own name, as an area
type. The other two are not offered at all.

Filters that need data this database does not have yet — age, weather,
hazards — are listed in PENDING so the page can show them as unavailable
rather than silently omitting them or, worse, approximating them.
"""
from __future__ import annotations

from bisect import bisect_left, bisect_right

# ── Area type, from MEASURED density ────────────────────────────────
# zips.db stores people per km²; people think in square miles.
SQMI_PER_KM2 = 2.589988

# Cut points in people per square mile. The Census Bureau's own urban
# definitions sit at 500 (fringe) and ~1,000 (core); 3,000 is where a ZIP
# starts to read as city blocks rather than subdivisions. Among the ZIPs this
# board can underwrite, these split roughly a quarter rural, two-fifths
# suburban, a third urban. They are judgement, recorded in DECISIONS.md.
AREA_TYPES = (
    ("rural", "Rural & small town", 0.0, 500.0),
    ("suburban", "Suburban", 500.0, 3000.0),
    ("urban", "Urban", 3000.0, None),
)
AREA_KEYS = tuple(k for k, *_ in AREA_TYPES)
AREA_LABEL = {k: label for k, label, *_ in AREA_TYPES}


def per_sq_mi(density_km2) -> float | None:
    if not isinstance(density_km2, (int, float)) or density_km2 < 0:
        return None
    return density_km2 * SQMI_PER_KM2


def area_type(density_km2) -> str | None:
    """rural / suburban / urban, or None when density is unknown.

    A ZIP-AVERAGE density: a ZIP holding a dense town plus its farmland
    reads as suburban. That is a real limitation of the unit, not of the
    arithmetic, and the page says so where the filter is offered.
    """
    d = per_sq_mi(density_km2)
    if d is None:
        return None
    # Rounded before banding: the km² -> sq mi round trip turns an exact 500
    # into 499.99999999999994, which would put a boundary ZIP in the wrong
    # band on float noise. A tenth of a person per square mile is past any
    # precision the source has.
    d = round(d, 1)
    for key, _label, lo, hi in AREA_TYPES:
        if d >= lo and (hi is None or d < hi):
            return key
    return None


# ── Filters, as data ────────────────────────────────────────────────
# Each filter compares ONE row field against ONE threshold. `choices` is the
# closed list of thresholds the page offers; anything else in a URL is
# ignored rather than trusted, so a hand-edited query string cannot ask for
# a threshold the page never promised to honour. "" means off.
FILTERS = (
    {"key": "min_income", "field": "median_household_income", "op": "min",
     "label": "Household income", "unit": "$", "group": "neighborhood",
     "choices": (50_000, 75_000, 100_000, 125_000),
     "why": "Median household income in the ZIP (Census ACS)."},
    {"key": "min_degree", "field": "pct_bachelors", "op": "min",
     "label": "Adults with a bachelor's degree", "unit": "%", "group": "neighborhood",
     "choices": (25, 35, 50),
     "why": "Share of adults 25+ with a bachelor's degree or higher (Census ACS)."},
    {"key": "max_cost", "field": "house_hack_net", "op": "max",
     "label": "Your monthly cost after rent", "unit": "$/mo", "group": "money",
     "choices": (0, 500, 1000, 1500),
     "choice_labels": {0: "Tenants cover it ($0 or less)"},
     "why": "PITI minus rent from the other units. 0 means the tenants cover it."},
    {"key": "min_cap", "field": "cap_rate_pct", "op": "min",
     "label": "Cap rate", "unit": "%", "group": "money",
     "choices": (4, 5, 6, 7),
     "why": "Gross rent yield on the ZIP's median value, at measured Zillow rents."},
    {"key": "min_trend", "field": "trend_3yr", "op": "min",
     "label": "3-yr price trend", "unit": "%/yr", "group": "money",
     "choices": (0, 2, 4),
     "choice_labels": {0: "Not falling (0%/yr or better)"},
     "why": "Annualised change in the ZIP's own home values over 36 months. "
            "Measured, not forecast."},
    # Available only when the Census columns carry data — see available().
    {"key": "min_renter", "field": "pct_renter_occupied", "op": "min",
     "label": "Renter share", "unit": "%", "group": "neighborhood",
     "choices": (30, 40, 50),
     "why": "Share of occupied homes that are rented (Census ACS).",
     "needs_column": "pct_renter_occupied"},
    {"key": "min_multi", "field": "pct_multi_unit", "op": "min",
     "label": "Multi-unit housing (2+ units)", "unit": "%", "group": "neighborhood",
     "choices": (15, 25, 35),
     "why": "Share of homes in buildings of 2 or more units — includes large "
            "apartment blocks, so it is broader than 2–4 unit stock (Census ACS).",
     "needs_column": "pct_multi_unit"},
)
FILTER_BY_KEY = {f["key"]: f for f in FILTERS}


def available(filt: dict, columns_with_data: set) -> bool:
    """Is this filter backed by data right now?

    The two Census filters light up on their own the first month the ACS
    fetch succeeds, with no deploy — and until then they are listed as
    pending instead of being offered over an empty column, where every row
    would read as NO DATA and the filter would silently empty the board.
    """
    need = filt.get("needs_column")
    return need is None or need in columns_with_data


# Filters that need data the database does not have. Shown on the page so the
# absence is visible, never approximated. The wording is for a public page —
# what is missing, not why an operator has not loaded it.
PENDING = (
    {"key": "young_pro", "label": "Young professional",
     "needs": "age 25–34 and renter share from the Census"},
    {"key": "stock_2_4", "label": "2–4 unit buildings",
     "needs": "a count of 2–4 unit buildings from the Census"},
    {"key": "weather", "label": "Weather",
     "needs": "NOAA climate normals — winter lows, summer highs, snow"},
    {"key": "hazards", "label": "Flood & wildfire",
     "needs": "the FEMA National Risk Index"},
    {"key": "schools", "label": "Schools",
     "needs": "a free, honest school-quality measure — none exists"},
)


def _num(v) -> float | None:
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        return None
    return float(v)


def parse_filters(params: dict) -> dict:
    """Query params -> {key: threshold or None, "area": [keys]}.

    Tolerant by construction: unknown values become "off", never an error,
    and never a threshold the page did not offer. `area` accepts a list or
    a comma-separated string; an empty or all-invalid list means every area.
    """
    out = {}
    for f in FILTERS:
        raw = params.get(f["key"])
        try:
            v = float(str(raw).strip()) if raw not in (None, "") else None
        except (TypeError, ValueError):
            v = None
        out[f["key"]] = v if v is not None and v in f["choices"] else None

    raw_area = params.get("area") or []
    if isinstance(raw_area, str):
        raw_area = raw_area.split(",")
    picked = [a.strip() for a in raw_area if isinstance(a, str)]
    picked = [a for a in AREA_KEYS if a in picked]      # canonical order, deduped
    # All three ticked is the same as none ticked: no filter. Normalising it
    # to [] keeps the funnel from showing a stage that removed nothing.
    out["area"] = [] if len(picked) in (0, len(AREA_KEYS)) else picked
    return out


def active_filters(filters: dict) -> list:
    """The filter specs that are switched on, in display order."""
    return [f for f in FILTERS if filters.get(f["key"]) is not None]


# ── The funnel ──────────────────────────────────────────────────────
class Funnel:
    """Every removal between "ZIPs in this state" and "ZIPs on the board".

    A board that falls from 1,017 ZIPs to 3 without saying why reads as
    broken; the same board showing its arithmetic reads as strict. So every
    step that removes rows goes through here, and the invariant the tests
    hold is that the steps ADD UP: start minus everything removed is exactly
    what is left. A step that removed rows without recording them would
    break that, which is the point.
    """

    def __init__(self, label: str, rows: list):
        self.start = {"label": label, "count": len(rows)}
        self.stages: list = []
        self.rows = list(rows)

    def step(self, key: str, label: str, keep, no_data=None,
             kind: str = "system") -> list:
        """Keep rows where keep(row) is truthy.

        `no_data(row)` marks the removed rows that failed for want of a
        figure rather than on the merits; they are counted apart. A row is
        only ever counted once, under the step that removed it.

        `kind` is "system" (a gate the page always applies), "safety", or
        "filter" (one the user switched on). The page always shows the
        user's own filters, even at zero removed — "your income filter
        removed nothing" is an answer — and hides system steps that did
        nothing, which are not.
        """
        before = len(self.rows)
        kept, gap = [], 0
        for r in self.rows:
            if keep(r):
                kept.append(r)
            elif no_data is not None and no_data(r):
                gap += 1
        self.stages.append({"key": key, "label": label, "before": before,
                            "removed": before - len(kept), "no_data": gap,
                            "after": len(kept), "kind": kind})
        self.rows = kept
        return kept

    @property
    def end(self) -> int:
        return len(self.rows)

    def emptied_by(self) -> dict | None:
        """The step that took the board to zero, if it is empty."""
        for s in self.stages:
            if s["before"] > 0 and s["after"] == 0:
                return s
        return None

    def balances(self) -> bool:
        return self.start["count"] - sum(s["removed"] for s in self.stages) == self.end


def apply_filters(funnel: Funnel, filters: dict) -> list:
    """Run the active user filters through the funnel, one stage each."""
    if filters.get("area"):
        allowed = set(filters["area"])
        names = " or ".join(AREA_LABEL[a].lower() for a in filters["area"])
        funnel.step("area", f"not {names}",
                    keep=lambda r: r.get("area_type") in allowed,
                    no_data=lambda r: r.get("area_type") is None, kind="filter")

    for f in active_filters(filters):
        t, field = filters[f["key"]], f["field"]
        if f["op"] == "min":
            keep = (lambda r, fld=field, t=t:
                    _num(r.get(fld)) is not None and _num(r.get(fld)) >= t)
            desc = f"{f['label'].lower()} below {fmt_threshold(f, t)}"
        else:
            keep = (lambda r, fld=field, t=t:
                    _num(r.get(fld)) is not None and _num(r.get(fld)) <= t)
            desc = f"{f['label'].lower()} above {fmt_threshold(f, t)}"
        funnel.step(f["key"], desc, keep=keep,
                    no_data=lambda r, fld=field: _num(r.get(fld)) is None,
                    kind="filter")
    return funnel.rows


def choice_label(f: dict, t: float) -> str:
    """How a threshold reads in the filter's dropdown."""
    special = f.get("choice_labels", {}).get(t)
    if special:
        return special
    return ("at least " if f["op"] == "min" else "at most ") + fmt_threshold(f, t)


def fmt_threshold(f: dict, t: float) -> str:
    if f["unit"] == "$":
        return f"${t:,.0f}"
    if f["unit"] == "$/mo":
        return f"${t:,.0f}/mo"
    if f["unit"] == "%/yr":
        return f"{t:g}%/yr"
    return f"{t:g}%"


# ── Priorities ──────────────────────────────────────────────────────
# (key, label, row field, lower_is_better)
PRIORITIES = (
    ("cashflow", "Cash flow", "house_hack_net", True),
    ("yield", "Rental yield", "cap_rate_pct", False),
    ("growth", "Price growth", "trend_3yr", False),
    ("income", "Neighborhood income", "median_household_income", False),
    ("education", "Educated neighbors", "pct_bachelors", False),
)
PRIORITY_KEYS = tuple(k for k, *_ in PRIORITIES)
WEIGHTS = (0, 1, 2, 3)
WEIGHT_LABELS = {0: "Ignore", 1: "A little", 2: "Important", 3: "Most"}

PRESETS = {
    "balanced": {"label": "Balanced",
                 "weights": {"cashflow": 3, "yield": 2, "growth": 2,
                             "income": 1, "education": 0}},
    "cashflow": {"label": "Cash flow first",
                 "weights": {"cashflow": 3, "yield": 3, "growth": 0,
                             "income": 0, "education": 0}},
    "growth": {"label": "Appreciation",
               "weights": {"cashflow": 1, "yield": 0, "growth": 3,
                           "income": 1, "education": 1}},
    "neighborhood": {"label": "Strong neighborhoods",
                     "weights": {"cashflow": 1, "yield": 0, "growth": 1,
                                 "income": 3, "education": 2}},
}
DEFAULT_PRESET = "balanced"


def parse_priorities(use_preset: str | None, params: dict) -> tuple:
    """-> (weights, preset key or "custom").

    A preset button submits `use_preset`, which wins outright. Otherwise the
    weights come from the w_* fields, and the preset shown is whichever one
    those weights match exactly — or "custom". No w_* fields at all (a first
    visit) is the default preset.
    """
    if use_preset in PRESETS:
        return dict(PRESETS[use_preset]["weights"]), use_preset

    submitted = {k: params.get(f"w_{k}") for k in PRIORITY_KEYS}
    if all(v in (None, "") for v in submitted.values()):
        return dict(PRESETS[DEFAULT_PRESET]["weights"]), DEFAULT_PRESET

    weights = {}
    for k, v in submitted.items():
        try:
            w = int(float(str(v).strip()))
        except (TypeError, ValueError):
            w = 0
        weights[k] = w if w in WEIGHTS else 0
    for key, p in PRESETS.items():
        if p["weights"] == weights:
            return weights, key
    return weights, "custom"


def percentiles(values: list, lower_is_better: bool = False) -> list:
    """Percentile 0–100 per value, None preserved. Ties share the midpoint.

    Midpoint ties matter here more than they look: two ZIPs with identical
    income must score identically, and bisect_left alone would hand the
    whole tie the bottom of its range. A single row scores 50 — it is
    neither best nor worst of one.
    """
    valid = sorted(v for v in values if v is not None)
    n = len(valid)
    out = []
    for v in values:
        if v is None:
            out.append(None)
            continue
        if n == 1:
            p = 50.0
        else:
            lo, hi = bisect_left(valid, v), bisect_right(valid, v) - 1
            p = (lo + hi) / 2 / (n - 1) * 100
        out.append(round(100 - p if lower_is_better else p, 1))
    return out


def board_order(rows: list) -> list:
    """Sort for display: every VERIFIED-safe row by score, then the rest.

    Rows whose safety nobody measured are on the board only when the user
    asked to see them, and even then they never rank above a measured one:
    a high score on an unmeasured ZIP would read as a recommendation the
    data cannot support. Within each group, best score first; a row with no
    score sorts last in its group rather than first.
    """
    def key(r):
        unmeasured = (r.get("safety") or {}).get("tier", "unknown") == "unknown"
        s = r.get("mf_score")
        return (unmeasured, s is None, -(s or 0.0))
    return sorted(rows, key=key)


def score(rows: list, weights: dict) -> dict:
    """Score each row 0–100 against the OTHER rows being shown.

    Percentiles are taken among the ZIPs that survived every filter — the
    candidates you would actually choose between — not the whole state.
    "Top of what you'd consider" is the question; "top of the state" would
    let ZIPs you had already ruled out set the curve.

    A row missing one signal is scored on the rest, reweighted, and says so
    in `score_basis`. Dropping it silently was the old behaviour; scoring the
    gap as zero would punish it for our missing data. Neither is honest.

    Returns {"weights": effective weights, "defaulted": bool}. All-zero
    weights are not a ranking, so they fall back to the default preset and
    report that they did.
    """
    active = {k: w for k, w in weights.items() if w}
    defaulted = False
    if not active:
        active = {k: w for k, w in PRESETS[DEFAULT_PRESET]["weights"].items() if w}
        defaulted = True

    cols = {}
    for key, _label, field, lower in PRIORITIES:
        if key in active:
            cols[key] = percentiles([_num(r.get(field)) for r in rows], lower)

    for i, r in enumerate(rows):
        parts = {k: cols[k][i] for k in cols}
        have = {k: p for k, p in parts.items() if p is not None}
        total_w = sum(active[k] for k in have)
        r["score_parts"] = parts
        r["score_basis"] = f"{len(have)} of {len(parts)}"
        r["score_complete"] = len(have) == len(parts)
        r["mf_score"] = (round(sum(active[k] * p for k, p in have.items()) / total_w, 1)
                         if total_w else None)
    return {"weights": active, "defaulted": defaulted}

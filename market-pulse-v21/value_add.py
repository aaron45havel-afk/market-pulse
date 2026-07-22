"""Value-Add — CA multifamily-that-needs-work finder. /value-add

Two layers (decided with the user, 2-person team / $200k):

  1. HUNTING GROUNDS — rank every CA ZIP by how likely it is to hold
     needs-work 2-4 unit buildings whose value-add math works. This is
     deliberately NOT the strict screen: forced-equity hunting lives
     exactly where the strict gates refuse to (old stock, cheap per
     unit, high yields). Signals, all from zips.db:
       • 2-4 unit density (ACS)          — can't buy a duplex where none exist
       • gross yield (cap_rate_pct)      — the rent engine that makes rehab pay
       • $ discount vs county median     — cheap for the area ≈ condition
       • renter share (ACS)              — tenant pool + exit liquidity
       • old stock (pct_pre_1960 /
         median_year_built)              — deferred-maintenance probability;
                                           populates on the next monthly
                                           national-zips rebuild, gracefully
                                           absent until then
     Plus non-gating context flags: rent-control jurisdiction, soft-story
     retrofit mandate, crime (information, not exclusion).

  2. REHAB CHECKER — 203(k)-house-hack-first underwriting for a pasted
     listing: as-is price + units + rehab budget + expected rents →
       • FHA 203(k): 3.5% down on (price + rehab), vs county FHA 2-4
         unit limits, cash to close vs deployable assets, post-reno
         PITI vs (n-1) rents, income to qualify, 3-4 unit
         self-sufficiency test
       • Equity created: income-approach ARV (post-reno gross rent ÷
         county yield benchmark) minus all-in — judged vs the 70-75%
         rule (DISCOUNT / FAIR / THIN)
       • BRRRR test: cash returned by a 75%-of-ARV refi
       • CA cost adders: rent control, soft-story, pre-1978 lead

ZIP-level ARV is a first-pass underwrite, not an appraisal — the UI
says so. Sourcing is manual by design: keyword-search Zillow/Redfin in
the target ZIPs (fixer, as-is, TLC, probate, estate) — the durable,
TOS-clean, 2-person-maintainable approach.
"""
from __future__ import annotations

import sqlite3
import statistics
from pathlib import Path

from norcal import _region  # county → NorCal / SoCal / San Diego

# ── SFR remodel budget model ─────────────────────────────────────────
# California / SF Bay Area 2025-26 unit costs (labor premium already baked
# in — no extra regional multiplier). Built from multi-source cost research
# then hardened by an adversarial review against a real change-of-occupancy
# conversion (516 Ward St, Martinez: 1922 former motorcycle shop → dwelling).
# The review pushed up contingency, made permits change-of-use aware, and
# itemized the conversion scope (sprinklers, under-slab DWV, egress windows,
# storefront infill, capacity fees, environmental) instead of a vague adder.
#
# Each item: cost at low / mid / high FINISH level, in its `basis` unit.
#   basis: fixed | sqft (interior) | roof (1.2×interior) | ext (1.2×interior)
#          | bath | window
#   scopes: which extent tiers include it by default (cosmetic/moderate/gut)
#   only_if: an option flag that must be on for the item to apply
#   replaces: this item substitutes for another when its option is on
#   soft: a fee/soft cost — added on top, NOT marked up by GC O&P or contingency

REMODEL_SCOPES = ("cosmetic", "moderate", "gut")
REMODEL_LEVELS = ("low", "mid", "high")

REMODEL_ITEMS = [
    # ── Structural & shell ──
    {"key": "foundation_repair", "label": "Foundation repair (partial — piers / crack / jack)",
     "cat": "structural", "basis": "fixed", "low": 4400, "mid": 9900, "high": 16000, "scopes": ("gut",)},
    {"key": "foundation_replacement", "label": "Full foundation replacement (lift + new pour)",
     "cat": "structural", "basis": "sqft", "low": 30, "mid": 45, "high": 70, "scopes": (),
     "only_if": "foundation_replace", "replaces": "foundation_repair"},
    {"key": "seismic_retrofit", "label": "Seismic retrofit — bolt + cripple-wall brace",
     "cat": "structural", "basis": "fixed", "low": 5000, "mid": 9000, "high": 15000, "scopes": ("gut",)},
    {"key": "urm_masonry_retrofit", "label": "Unreinforced-masonry (URM) seismic retrofit",
     "cat": "structural", "basis": "sqft", "low": 45, "mid": 100, "high": 155, "scopes": (),
     "only_if": "masonry", "replaces": "seismic_retrofit"},
    {"key": "termite_dry_rot", "label": "Termite / dry-rot structural repair allowance",
     "cat": "structural", "basis": "fixed", "low": 1500, "mid": 4000, "high": 12000, "scopes": ("gut",)},
    {"key": "framing_repair", "label": "Framing repair / partial re-framing allowance",
     "cat": "structural", "basis": "fixed", "low": 3000, "mid": 8000, "high": 20000, "scopes": ("gut",)},
    {"key": "demolition", "label": "Gut demo to studs + debris haul-off",
     "cat": "structural", "basis": "sqft", "low": 4, "mid": 7, "high": 12, "scopes": ("gut",)},
    # ── Envelope ──
    {"key": "roof_replace", "label": "Roof tear-off + re-roof (composition)",
     "cat": "envelope", "basis": "roof", "low": 6, "mid": 8.5, "high": 12, "scopes": ("gut",)},
    {"key": "siding", "label": "Siding — fiber-cement, incl. strip old",
     "cat": "envelope", "basis": "ext", "low": 10, "mid": 14, "high": 18, "scopes": ("gut",)},
    {"key": "exterior_paint", "label": "Exterior repaint (prep + 2 coats)",
     "cat": "envelope", "basis": "ext", "low": 2.5, "mid": 4, "high": 6, "scopes": ("cosmetic", "moderate")},
    {"key": "windows", "label": "Replacement windows — vinyl retrofit",
     "cat": "envelope", "basis": "window", "low": 500, "mid": 700, "high": 950, "scopes": ("moderate", "gut")},
    {"key": "windows_egress", "label": "New cut-in window openings + egress (conversion)",
     "cat": "envelope", "basis": "window", "low": 1600, "mid": 2500, "high": 4000, "scopes": (),
     "only_if": "conversion", "replaces": "windows"},
    {"key": "exterior_doors", "label": "Exterior doors (~3, entry + patio)",
     "cat": "envelope", "basis": "fixed", "low": 2700, "mid": 4800, "high": 8400, "scopes": ("gut",)},
    {"key": "insulation", "label": "Whole-house insulation — walls + attic",
     "cat": "envelope", "basis": "sqft", "low": 1.5, "mid": 2.5, "high": 3.75, "scopes": ("gut",)},
    {"key": "gutters", "label": "Seamless gutters + downspouts",
     "cat": "envelope", "basis": "fixed", "low": 1600, "mid": 2340, "high": 3240, "scopes": ("gut",)},
    {"key": "storefront_infill", "label": "Storefront / roll-up door infill (conversion)",
     "cat": "envelope", "basis": "fixed", "low": 8000, "mid": 14000, "high": 22000, "scopes": (),
     "only_if": "conversion"},
    # ── Mechanical / electrical / plumbing ──
    {"key": "electrical_rewire", "label": "Full electrical rewire to code",
     "cat": "mep", "basis": "sqft", "low": 7, "mid": 11, "high": 16, "scopes": ("gut",)},
    {"key": "electrical_panel", "label": "200A service panel upgrade",
     "cat": "mep", "basis": "fixed", "low": 3000, "mid": 4500, "high": 7000, "scopes": ("moderate", "gut")},
    {"key": "plumbing_repipe", "label": "Whole-house PEX supply repipe",
     "cat": "mep", "basis": "fixed", "low": 10000, "mid": 15000, "high": 22000, "scopes": ("gut",)},
    {"key": "dwv_underslab", "label": "New DWV drainage / under-slab plumbing (conversion)",
     "cat": "mep", "basis": "fixed", "low": 8000, "mid": 12000, "high": 18000, "scopes": (),
     "only_if": "conversion"},
    {"key": "water_heater", "label": "Water heater (tank / tankless)",
     "cat": "mep", "basis": "fixed", "low": 2500, "mid": 3250, "high": 4000, "scopes": ("gut",)},
    {"key": "sewer_lateral", "label": "Sewer lateral replacement (house → main)",
     "cat": "mep", "basis": "fixed", "low": 4000, "mid": 8000, "high": 15000, "scopes": ("gut",)},
    {"key": "hvac", "label": "Forced-air HVAC + full ductwork",
     "cat": "mep", "basis": "fixed", "low": 14000, "mid": 20000, "high": 28000, "scopes": ("moderate", "gut")},
    {"key": "fire_sprinklers", "label": "Fire sprinklers (NFPA 13D — change of occupancy)",
     "cat": "mep", "basis": "fixed", "low": 6000, "mid": 10000, "high": 14000, "scopes": (),
     "only_if": "conversion"},
    # ── Interior finishes ──
    {"key": "kitchen", "label": "Full kitchen remodel",
     "cat": "interior", "basis": "fixed", "low": 30000, "mid": 62000, "high": 115000, "scopes": ("moderate", "gut")},
    {"key": "bathrooms", "label": "Full bathroom remodel",
     "cat": "interior", "basis": "bath", "low": 15000, "mid": 28000, "high": 55000, "scopes": ("moderate", "gut")},
    {"key": "drywall", "label": "Drywall hang, tape + finish",
     "cat": "interior", "basis": "sqft", "low": 3.5, "mid": 5, "high": 7, "scopes": ("gut",)},
    {"key": "flooring", "label": "Flooring — LVP / engineered / tile mix",
     "cat": "interior", "basis": "sqft", "low": 8, "mid": 13, "high": 22, "scopes": ("cosmetic", "moderate", "gut")},
    {"key": "interior_paint", "label": "Interior paint — walls, ceilings, trim",
     "cat": "interior", "basis": "sqft", "low": 2.5, "mid": 3.75, "high": 5.5, "scopes": ("cosmetic", "moderate", "gut")},
    {"key": "doors_trim", "label": "Interior doors, baseboard + casing",
     "cat": "interior", "basis": "sqft", "low": 3, "mid": 5, "high": 8, "scopes": ("gut",)},
    {"key": "lighting", "label": "Decorative lighting + finish fixtures",
     "cat": "interior", "basis": "fixed", "low": 2500, "mid": 5000, "high": 10000, "scopes": ("cosmetic", "moderate", "gut")},
    # ── Fees & specialty (soft — not marked up by GC / contingency) ──
    {"key": "change_of_use", "label": "Change-of-use permits + school & impact fees (conversion)",
     "cat": "fees", "basis": "fixed", "low": 30000, "mid": 40000, "high": 55000, "scopes": (),
     "only_if": "conversion", "soft": True},
    {"key": "sewer_capacity", "label": "Sanitary-district capacity fee (new dwelling)",
     "cat": "fees", "basis": "fixed", "low": 7200, "mid": 8200, "high": 9200, "scopes": (),
     "only_if": "conversion", "soft": True},
    {"key": "phase1_esa", "label": "Phase I environmental (former commercial/auto use)",
     "cat": "fees", "basis": "fixed", "low": 2500, "mid": 3000, "high": 4000, "scopes": (),
     "only_if": "conversion", "soft": True},
]

# Percentages stack on the HARD subtotal exactly in this order (validated to
# reproduce Bay-Area gut benchmarks): GC O&P on hard; contingency on hard+GC;
# design and permit each on hard. Abatement is a flat $/sqft on top.
REMODEL_GLOBALS = {
    "gc_op_pct": 22,
    "design_pct": {"base": 6, "conversion": 9},
    "permit_pct": 4,                              # skipped when conversion (change_of_use line instead)
    "contingency_pct": {"base": 15, "conversion": 18},
    "abatement_psf": {"low": 8, "mid": 12, "high": 18},   # pre-1978 lead/asbestos
}

# One-line all-in $/sqft benchmark per extent tier (Bay Area, for orientation).
REMODEL_TIER_PSF = {"cosmetic": 45, "moderate": 150, "gut": 275}
REMODEL_TIER_DESC = {
    "cosmetic": "Lipstick refresh — paint, flooring, and finish fixtures only; no cabinetry, no systems, nothing opened up.",
    "moderate": "Cosmetic plus a new kitchen & bath(s), 200A panel, HVAC, and windows — aging mechanicals addressed, walls mostly stay closed.",
    "gut": "Everything to the studs: demo, foundation/seismic as needed, roof, siding, full MEP, insulation, drywall, kitchen & baths, flooring, paint, trim.",
}


def _est_windows(sqft: float) -> int:
    return max(6, round(sqft / 130))


def _qty(basis: str, sqft: float, baths: float, windows: int) -> float:
    return {
        "fixed": 1.0, "sqft": sqft, "roof": sqft * 1.2, "ext": sqft * 1.2,
        "bath": max(1.0, baths), "window": float(windows),
    }.get(basis, 1.0)


def _active_items(scope: str, opts: dict) -> list[dict]:
    """The line items in play for this extent tier + options, after applying
    substitutions (URM→seismic, egress→windows, full→partial foundation)."""
    active = [it for it in REMODEL_ITEMS
              if (scope in it["scopes"] and not it.get("only_if"))
              or (it.get("only_if") and opts.get(it["only_if"]))]
    replaced = {it["replaces"] for it in active if it.get("replaces")}
    return [it for it in active if it["key"] not in replaced]


def remodel_budget(sqft: float, beds: int = 3, baths: float = 1, year_built: int | None = None,
                   scope: str = "gut", level: str = "mid", *,
                   conversion: bool = False, masonry: bool = False,
                   foundation_replace: bool = False, pre1978: bool | None = None,
                   windows: int | None = None) -> dict:
    """Build an itemized California SFR remodel budget → total rehab $.

    Returns the line items at the chosen finish `level`, the soft-cost stack,
    the total, and a low/mid/high band (finish level swept). All costs are
    Bay-Area 2025-26. First-pass underwriting, not a contractor bid."""
    sqft = max(200.0, float(sqft or 0))
    baths = max(1.0, float(baths or 1))
    scope = scope if scope in REMODEL_SCOPES else "gut"
    level = level if level in REMODEL_LEVELS else "mid"
    if pre1978 is None:
        pre1978 = bool(year_built and year_built < 1978)
    win = int(windows) if windows and int(windows) > 0 else _est_windows(sqft)
    opts = {"conversion": conversion, "masonry": masonry, "foundation_replace": foundation_replace}
    items = _active_items(scope, opts)

    def compute(lv: str) -> dict:
        hard_lines, soft_lines = [], []
        for it in items:
            q = _qty(it["basis"], sqft, baths, win)
            amt = it[lv] * q
            row = {"key": it["key"], "label": it["label"], "cat": it["cat"],
                   "qty": q, "basis": it["basis"], "amount": round(amt)}
            (soft_lines if it.get("soft") else hard_lines).append(row)
        hard = sum(r["amount"] for r in hard_lines)
        g = REMODEL_GLOBALS
        cont_pct = g["contingency_pct"]["conversion" if conversion else "base"]
        design_pct = g["design_pct"]["conversion" if conversion else "base"]
        gc = hard * g["gc_op_pct"] / 100
        contingency = (hard + gc) * cont_pct / 100
        design = hard * design_pct / 100
        permit = 0.0 if conversion else hard * g["permit_pct"] / 100
        abatement = sqft * g["abatement_psf"][lv] if pre1978 else 0.0
        soft_fee_sum = sum(r["amount"] for r in soft_lines)
        addons = [
            {"key": "gc_op", "label": f"General contractor overhead & profit ({g['gc_op_pct']}%)", "amount": round(gc)},
            {"key": "contingency", "label": f"Contingency reserve ({cont_pct}%)", "amount": round(contingency)},
            {"key": "design", "label": f"Design / architecture / engineering ({design_pct}%)", "amount": round(design)},
        ]
        if not conversion:
            addons.append({"key": "permit", "label": f"Building permits & fees ({g['permit_pct']}%)", "amount": round(permit)})
        if pre1978:
            addons.append({"key": "abatement", "label": f"Lead / asbestos abatement (pre-1978, ${g['abatement_psf'][lv]}/sqft)", "amount": round(abatement)})
        total = hard + gc + contingency + design + permit + abatement + soft_fee_sum
        return {"hard_lines": hard_lines, "soft_lines": soft_lines, "addons": addons,
                "hard": round(hard), "total": round(total)}

    sel = compute(level)
    band = {lv: compute(lv)["total"] for lv in REMODEL_LEVELS}

    flags = []
    if conversion:
        flags.append("Commercial → residential conversion: change-of-use permits, egress, DWV, sprinklers, and a sanitary capacity fee are included. A former auto/repair use also carries soil-contamination risk beyond the Phase I — hold a remediation reserve.")
    if pre1978:
        flags.append("Pre-1978: lead & asbestos handling is priced in; test before demo.")
    if year_built and year_built < 1950 and not masonry:
        flags.append("Pre-1950 structure: confirm construction type — if unreinforced masonry, switch on the URM option (wood-frame retrofit pricing won't cover it).")

    return {
        "sqft": round(sqft), "beds": beds, "baths": baths, "year_built": year_built,
        "scope": scope, "level": level, "windows": win,
        "conversion": conversion, "masonry": masonry, "foundation_replace": foundation_replace, "pre1978": pre1978,
        "hard_lines": sel["hard_lines"], "soft_lines": sel["soft_lines"], "addons": sel["addons"],
        "hard": sel["hard"], "total": sel["total"], "band": band,
        "psf": round(sel["total"] / sqft),
        "tier_desc": REMODEL_TIER_DESC[scope], "tier_psf": REMODEL_TIER_PSF[scope],
        "flags": flags,
    }

_ZIPS_DB = Path(__file__).resolve().parent / "data" / "zips.db"

ASSETS_DEFAULT = 200_000
RESERVES_DEFAULT = 40_000
RATE_DEFAULT = 6.5           # 30-yr fixed %
MIP_ANNUAL = 0.55            # FHA annual MIP %, life-of-loan under 10% down
REHAB_CARRY_MONTHS = 6       # interest carried during renovation
CLOSING_PCT = 2.0            # closing costs (FHA allows some roll-in; cash-side estimate)

SEARCH_KEYWORDS = "fixer, as-is, TLC, needs work, probate, estate sale, contractor special"

# Local ordinances STRICTER than statewide AB 1482 (which caps increases
# at 5%+CPI ≤10% on most 15+ year-old buildings everywhere in CA).
RENT_CONTROL_CITIES = {
    "San Francisco", "Oakland", "Berkeley", "San Jose", "Los Angeles",
    "Santa Monica", "West Hollywood", "Beverly Hills", "East Palo Alto",
    "Hayward", "Alameda", "Richmond", "Mountain View", "Inglewood",
    "Culver City", "Pasadena", "Sacramento", "Baldwin Park", "Pomona",
    "Santa Ana", "Bell Gardens",
}
# Cities with mandatory soft-story seismic retrofit programs — a known
# five-to-six-figure cost on pre-1980 wood-frame multifamily.
SOFT_STORY_CITIES = {
    "San Francisco", "Los Angeles", "Oakland", "Berkeley", "Santa Monica",
    "West Hollywood", "Pasadena", "San Jose", "Torrance", "Culver City",
    "Beverly Hills",
}

# FHA 2026-ish loan limits (approx — update annually). 1-unit base by
# county tier; 2-4 unit limits derived with FHA's standard ratios.
_FHA_1U_CEILING = 1_209_750     # high-cost CA counties
_FHA_1U_FLOOR = 524_225
_FHA_COUNTY_1U = {
    "San Francisco County": _FHA_1U_CEILING, "San Mateo County": _FHA_1U_CEILING,
    "Santa Clara County": _FHA_1U_CEILING, "Marin County": _FHA_1U_CEILING,
    "Alameda County": _FHA_1U_CEILING, "Contra Costa County": _FHA_1U_CEILING,
    "Santa Cruz County": _FHA_1U_CEILING, "Los Angeles County": _FHA_1U_CEILING,
    "Orange County": _FHA_1U_CEILING, "San Benito County": _FHA_1U_CEILING,
    "Napa County": 1_017_750, "San Diego County": 1_006_250,
    "Ventura County": 954_500, "Sonoma County": 897_000,
    "Monterey County": 920_000, "San Luis Obispo County": 967_150,
    "Santa Barbara County": 838_350, "Solano County": 685_400,
    "Sacramento County": 763_600, "Placer County": 763_600,
    "El Dorado County": 763_600, "Yolo County": 763_600,
}
_FHA_UNIT_RATIO = {2: 1.2802, 3: 1.5475, 4: 1.9231}


def fha_limit(county: str | None, units: int) -> int:
    base = _FHA_COUNTY_1U.get(county or "", _FHA_1U_FLOOR)
    return round(base * _FHA_UNIT_RATIO.get(units, 1.0))


def _pct_rank(values: list[float], v: float | None) -> float | None:
    """v's percentile (0-100) within values (higher v → higher pct)."""
    if v is None or not values:
        return None
    below = sum(1 for x in values if x < v)
    return round(below / len(values) * 100, 1)


def _universe(conn) -> list[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    cols = {r[1] for r in conn.execute("PRAGMA table_info(zips)")}
    extra = ""
    if "pct_pre_1960" in cols:
        extra += ", pct_pre_1960"
    if "median_year_built" in cols:
        extra += ", median_year_built"
    return conn.execute(f"""
        SELECT zip, name, county, population, population_density,
               median_home_value, median_rent_monthly, cap_rate_pct,
               pct_renter_occupied, pct_multi_unit, pct_rent_burdened,
               crime_index {extra}
        FROM zips
        WHERE state = 'CA' AND population > 3000
          AND median_home_value IS NOT NULL
          AND median_rent_monthly IS NOT NULL
    """).fetchall()


def _flags(city: str, county: str | None, pre60, myb) -> list[dict]:
    out = []
    rc = next((c for c in RENT_CONTROL_CITIES if c in city), None)
    if rc:
        out.append({"key": "rentctl", "label": "rent control",
                    "title": f"{rc} has local rent control stricter than AB 1482 — the raise-to-market-after-reno play is constrained. Vacancy decontrol still applies on turnover."})
    ss = next((c for c in SOFT_STORY_CITIES if c in city), None)
    if ss:
        out.append({"key": "softstory", "label": "soft-story",
                    "title": f"{ss} mandates seismic retrofit of pre-1980 soft-story multifamily — budget a known five-to-six figure line item if the building qualifies."})
    if (pre60 is not None and pre60 >= 50) or (myb is not None and myb < 1960):
        out.append({"key": "old", "label": "pre-1960 stock",
                    "title": "Majority of stock predates 1960 — deferred maintenance likely, and any rehab budget carries pre-1978 lead/asbestos handling costs."})
    return out


def hunting_grounds(region: str = "All CA", limit: int = 60) -> dict:
    """Ranked value-add hunt ZIPs. Score = weighted percentile blend
    within the CA universe; old-stock term joins automatically once the
    year-built columns exist."""
    if not _ZIPS_DB.exists():
        return {"rows": [], "universe_n": 0, "has_age": False, "region": region}
    conn = sqlite3.connect(str(_ZIPS_DB))
    try:
        rows = _universe(conn)
    finally:
        conn.close()

    has_age = rows and "pct_pre_1960" in rows[0].keys()

    # County median $ (all CA rows) for the discount term.
    by_county: dict[str, list[float]] = {}
    for r in rows:
        if r["median_home_value"]:
            by_county.setdefault(r["county"] or "?", []).append(r["median_home_value"])
    county_med = {c: statistics.median(v) for c, v in by_county.items() if len(v) >= 3}

    caps = [r["cap_rate_pct"] for r in rows if r["cap_rate_pct"] is not None]
    dens = [r["population_density"] for r in rows if r["population_density"] is not None]
    multis = [r["pct_multi_unit"] for r in rows if r["pct_multi_unit"] is not None]
    renters = [r["pct_renter_occupied"] for r in rows if r["pct_renter_occupied"] is not None]
    pre60s = [r["pct_pre_1960"] for r in rows if has_age and r["pct_pre_1960"] is not None]

    scored = []
    for r in rows:
        reg = _region(r["county"])
        if region != "All CA" and reg != region:
            continue
        cmed = county_med.get(r["county"] or "?")
        discount = ((cmed - r["median_home_value"]) / cmed * 100) if cmed else None

        p_cap = _pct_rank(caps, r["cap_rate_pct"])
        p_dens = _pct_rank(dens, r["population_density"])
        p_multi = _pct_rank(multis, r["pct_multi_unit"])
        p_renter = _pct_rank(renters, r["pct_renter_occupied"])
        p_disc = max(0.0, min(100.0, 50 + (discount or 0) * 1.2)) if discount is not None else None
        pre60 = r["pct_pre_1960"] if has_age else None
        myb = r["median_year_built"] if has_age else None
        p_age = _pct_rank(pre60s, pre60) if has_age else None

        # ACS multifamily/renter/age terms join the blend when present
        # (CA rows are currently NULL there — the yield/discount/density
        # core carries the score until the next national-zips rebuild).
        parts = {"yield": (p_cap, 0.35), "discount": (p_disc, 0.30),
                 "density": (p_dens, 0.15),
                 "multi": (p_multi, 0.20), "renter": (p_renter, 0.10)}
        if p_age is not None:
            parts["age"] = (p_age, 0.15)
        avail = {k: (v, w) for k, (v, w) in parts.items() if v is not None}
        if len(avail) < 2:
            continue
        wsum = sum(w for _, w in avail.values())
        score = round(sum(v * w for v, w in avail.values()) / wsum, 1)

        city = (r["name"] or "").replace(", CA", "")
        scored.append({
            "zip": r["zip"], "name": city, "county": r["county"], "region": reg,
            "population": r["population"], "score": score,
            "median_home_value": r["median_home_value"],
            "median_rent": r["median_rent_monthly"],
            "cap_rate_pct": r["cap_rate_pct"],
            "pct_multi_unit": r["pct_multi_unit"],
            "pct_renter": r["pct_renter_occupied"],
            "discount_pct": round(discount, 1) if discount is not None else None,
            "pct_pre_1960": pre60, "median_year_built": myb,
            "crime": r["crime_index"],
            "flags": _flags(city, r["county"], pre60, myb),
        })
    scored.sort(key=lambda s: -s["score"])
    return {"rows": scored[:limit], "universe_n": len(scored),
            "has_age": bool(has_age), "region": region,
            "keywords": SEARCH_KEYWORDS}


# ── Rehab checker ────────────────────────────────────────────────────

def _piti(loan: float, rate_pct: float, price: float, mip: bool) -> dict:
    r = rate_pct / 100 / 12
    n = 360
    p_i = loan * (r * (1 + r) ** n) / ((1 + r) ** n - 1) if r > 0 else loan / n
    tax = price * 0.0125 / 12
    ins = 3600 / 12                       # multifamily hazard ballpark
    mip_mo = loan * MIP_ANNUAL / 100 / 12 if mip else 0.0
    return {"p_i": p_i, "tax": tax, "ins": ins, "mip": mip_mo,
            "piti": p_i + tax + ins + mip_mo}


def rehab_check(zip_code: str, price: float, units: int, rehab: float,
                rent_unit: float | None = None, income: float | None = None,
                assets: float = ASSETS_DEFAULT, rate_pct: float = RATE_DEFAULT) -> dict | None:
    """Underwrite one needs-work listing, 203(k)-house-hack first."""
    if not _ZIPS_DB.exists():
        return None
    conn = sqlite3.connect(str(_ZIPS_DB))
    conn.row_factory = sqlite3.Row
    try:
        r = conn.execute(
            "SELECT * FROM zips WHERE zip = ? AND state = 'CA'", (zip_code,)).fetchone()
        if r is None:
            return None
        cty_rows = conn.execute(
            "SELECT cap_rate_pct FROM zips WHERE county = ? AND state='CA' "
            "AND cap_rate_pct IS NOT NULL", (r["county"],)).fetchall()
    finally:
        conn.close()

    units = max(2, min(4, units))
    market_rent = rent_unit or r["median_rent_monthly"] or 0
    county_yield = statistics.median([x["cap_rate_pct"] for x in cty_rows]) \
        if cty_rows else (r["cap_rate_pct"] or 6.0)
    county_yield = max(3.0, min(15.0, county_yield))

    # ── ARV: LOWER of two approaches (lender-style conservatism) ──
    # Comp approach: 2-4 unit buildings trade at a multiple of the same
    # ZIP's single-family median (≈1.4x duplex / 1.7x triplex / 2.0x
    # fourplex). Income approach: post-reno gross rent ÷ the ZIP's own
    # gross yield, floored at 4.5% so expensive-neighborhood yields
    # can't inflate ARV. Taking the min keeps the equity verdict honest.
    unit_mult = {2: 1.40, 3: 1.70, 4: 2.00}[units]
    comp_arv = (r["median_home_value"] or 0) * unit_mult or None
    own_yield = max((r["cap_rate_pct"] or county_yield or 4.5), 4.5)
    gross_annual = market_rent * units * 12
    income_arv = gross_annual / (own_yield / 100)
    candidates = [a for a in (comp_arv, income_arv) if a]
    arv = min(candidates) if candidates else None
    carry = (price + rehab) * 0.965 * (rate_pct / 100) * (REHAB_CARRY_MONTHS / 12)
    all_in = price + rehab + carry
    equity = (arv - all_in) if arv else None
    margin = (equity / arv * 100) if (arv and equity is not None) else None
    if margin is None:
        verdict = "NO BENCHMARK"
    elif margin >= 20:
        verdict = "DISCOUNT"
    elif margin >= 8:
        verdict = "FAIR"
    else:
        verdict = "THIN"

    # ── FHA 203(k) house-hack ──
    total_project = price + rehab
    limit = fha_limit(r["county"], units)
    fha_ok = total_project * 0.965 <= limit
    loan_fha = total_project * 0.965
    down_fha = total_project * 0.035
    cash_close = down_fha + total_project * CLOSING_PCT / 100
    pf = _piti(loan_fha, rate_pct, total_project, mip=True)
    hh_rent = market_rent * (units - 1)
    hh_net = pf["piti"] - hh_rent
    income_needed = pf["piti"] * 12 / 0.28
    qualifies = (income is not None and income > 0 and income * 0.28 / 12 >= pf["piti"]) \
        if income else None
    self_suff = None
    if units >= 3:
        self_suff = 0.75 * market_rent * units >= pf["piti"]

    # ── BRRRR (investor exit) ──
    refi = 0.75 * arv if arv else None
    cash_in = down_fha + rehab * 0  # 203(k) rolls rehab; investor path below
    inv_down = price * 0.25
    inv_cash = inv_down + rehab + carry
    brrrr_left = (inv_cash - (refi - price * 0.75)) if refi else None

    city = (r["name"] or "").replace(", CA", "")
    return {
        "zip": zip_code, "name": city, "county": r["county"],
        "region": _region(r["county"]),
        "units": units, "price": price, "rehab": rehab,
        "market_rent": market_rent, "county_yield": round(own_yield, 2), "arv_comp": round(comp_arv) if comp_arv else None, "arv_income": round(income_arv) if income_arv else None,
        "arv": round(arv) if arv else None,
        "all_in": round(all_in), "carry": round(carry),
        "equity": round(equity) if equity is not None else None,
        "margin": round(margin, 1) if margin is not None else None,
        "verdict": verdict,
        "fha": {"limit": limit, "ok": fha_ok, "down": round(down_fha),
                "cash_close": round(cash_close),
                "cash_ok": cash_close <= max(0, assets - RESERVES_DEFAULT),
                "piti": round(pf["piti"]), "hh_rent": round(hh_rent),
                "hh_net": round(hh_net), "income_needed": round(income_needed),
                "qualifies": qualifies, "self_suff": self_suff},
        "brrrr": {"refi": round(refi) if refi else None,
                  "inv_cash": round(inv_cash),
                  "left_in": round(brrrr_left) if brrrr_left is not None else None},
        "flags": _flags(city, r["county"],
                        r["pct_pre_1960"] if "pct_pre_1960" in r.keys() else None,
                        r["median_year_built"] if "median_year_built" in r.keys() else None),
        "income": income, "assets": assets, "rate_pct": rate_pct,
    }

"""Structural-shift signals for the real-estate pages.

The problem this module solves: every screen in the app ranks on
*levels* (today's cap rate, today's composite score) and implicitly
assumes mean reversion. Structural shifts — population decline,
insurance repricing, supply gluts, value deceleration — are exactly
when mean reversion fails. A 24% gross cap rate in a shrinking metro
isn't mispricing; it's the market charging a structural-risk premium.

Three tools, shared by /multifamily and /map:

1. trajectory_from_history()  — turns a ZIP's 60-month ZHVI series
   into 1yr/3yr trends + a second derivative (deceleration), and a
   label: accelerating / steady / decelerating / declining.
2. durable_cap_rate()         — gross rent yield minus the expenses
   that structural risk actually shows up in: vacancy, property tax,
   insurance (state-level $, which hammers low-value ZIPs — that's
   the honest math), and a maintenance reserve.
3. state_structural()         — flag-level regime signals from the
   STATES table: insurance shock, population decline, out-migration,
   supply glut (permits outrunning demand), falling values.

Principle: trajectory can VETO a level. A great level score with a
bad trajectory gets capped, visibly, rather than silently averaged.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import statistics
from functools import lru_cache
from pathlib import Path

logger = logging.getLogger(__name__)

_ZIPS_DB = Path(__file__).resolve().parent / "data" / "zips.db"

# ── Trajectory from ZHVI history ─────────────────────────────────────

# Thresholds (annualized %-point gap between the trailing year and the
# two years before it). ±2-3 pts of ZHVI drift is normal noise; beyond
# that the second derivative is telling you the regime is changing.
DECEL_THRESHOLD = -3.0   # 1yr running this far below prior trend = decelerating
ACCEL_THRESHOLD = 2.0


def trajectory_from_history(history: list | None) -> dict | None:
    """Trend + second derivative from a monthly ZHVI series (oldest
    first, ~60 points). Returns None when there's not enough data to
    say anything (needs 37 clean months: 12 trailing + 24 prior + 1).

      cagr_3yr_pct   — annualized change over the last 36 months
      chg_1yr_pct    — last 12 months
      prior_2yr_pct  — annualized months -36..-12 (the base trend)
      decel_pct      — chg_1yr - prior_2yr. Negative = losing steam.
      label          — declining | decelerating | steady | accelerating
    """
    if not history:
        return None
    pts = [float(v) for v in history if v is not None]
    if len(pts) < 37 or pts[-1] <= 0:
        return None

    last, yr1, yr3 = pts[-1], pts[-13], pts[-37]
    if yr1 <= 0 or yr3 <= 0:
        return None

    cagr_3yr = ((last / yr3) ** (1 / 3) - 1) * 100
    chg_1yr = (last / yr1 - 1) * 100
    prior_2yr = ((yr1 / yr3) ** 0.5 - 1) * 100
    decel = chg_1yr - prior_2yr

    if cagr_3yr < 0 and chg_1yr <= 0:
        label = "declining"
    elif decel <= DECEL_THRESHOLD and chg_1yr < prior_2yr:
        label = "decelerating"
    elif decel >= ACCEL_THRESHOLD:
        label = "accelerating"
    else:
        label = "steady"

    return {
        "cagr_3yr_pct":  round(cagr_3yr, 1),
        "chg_1yr_pct":   round(chg_1yr, 1),
        "prior_2yr_pct": round(prior_2yr, 1),
        "decel_pct":     round(decel, 1),
        "label":         label,
    }


# Display metadata so both templates render trajectories identically.
TRAJECTORY_BADGES = {
    "accelerating": {"glyph": "▲", "cls": "traj-up",    "label": "Accelerating"},
    "steady":       {"glyph": "→", "cls": "traj-flat",  "label": "Steady"},
    "decelerating": {"glyph": "⚠", "cls": "traj-warn",  "label": "Decelerating"},
    "declining":    {"glyph": "▼", "cls": "traj-down",  "label": "Declining"},
}


# ── Durable cap rate ─────────────────────────────────────────────────

VACANCY_FACTOR = 0.92        # 8% of gross rent lost to vacancy/turnover
MAINTENANCE_PCT = 1.5        # annual reserve as % of value (older cheap
                             # stock burns more; this is a middle estimate)
_MIN_VALUE_FOR_BURDEN = 30_000   # floor so a $15k row doesn't show a 12% ins. load


def durable_cap_rate(gross_cap_pct: float | None, home_value: float | None,
                     state_code: str) -> dict | None:
    """Gross rent yield minus the carrying costs that structural risk
    hides in. Returns the net rate plus each component so the UI can
    show its work.

    durable = gross×0.92 − property_tax% − insurance$/value% − 1.5% maint.

    The insurance term is the structural teeth: a fixed state-level
    dollar premium is a huge % drag on a $52k Toledo duplex and a
    rounding error on a $500k Austin one.
    """
    if gross_cap_pct is None or not home_value or home_value <= 0:
        return None
    st = _states().get(state_code.upper())
    if not st:
        return None
    value = max(float(home_value), _MIN_VALUE_FOR_BURDEN)
    tax_pct = float(st.get("property_tax") or 1.0)
    ins_pct = float(st.get("insurance") or 1500) / value * 100
    net = gross_cap_pct * VACANCY_FACTOR - tax_pct - ins_pct - MAINTENANCE_PCT
    return {
        "durable_cap_pct": round(net, 1),
        "gross_cap_pct":   round(gross_cap_pct, 1),
        "vacancy_drag":    round(gross_cap_pct * (1 - VACANCY_FACTOR), 1),
        "tax_pct":         round(tax_pct, 2),
        "insurance_pct":   round(ins_pct, 2),
        "maintenance_pct": MAINTENANCE_PCT,
    }


# ── State-level structural flags ─────────────────────────────────────

# Flag definitions: (key, short chip label, longer tooltip).
FLAG_META = {
    "insurance_shock": ("🌀 Insurance",   "Home insurance is repricing structurally here — premiums ≥ $3,500/yr or ≥ 1% of the median home value."),
    "pop_decline":     ("▼ Population",   "Population growth ≤ 0 — a shrinking demand base eventually hits rents and occupancy, not just prices."),
    "out_migration":   ("⇤ Migration",    "Strong net out-migration (≤ −5/1k). Trailing yields may overstate what the next decade supports."),
    "supply_glut":     ("⚠ Supply",       "Permits ≥ 5.5/1k residents while values fall — supply is outrunning demand (the post-boom Austin pattern)."),
    "values_falling":  ("▼ Values",       "State median home value down ≥ 2% YoY."),
}


def state_structural(state_code: str) -> dict:
    """Structural regime read for one state, from the STATES table.
    Returns {'flags': [keys], 'chips': [{key,label,title}], plus the
    raw inputs the flags derive from} — empty flags for a healthy state."""
    st = _states().get(state_code.upper())
    if not st:
        return {"flags": [], "chips": []}
    home_value = float(st.get("home_value") or 0) or 1
    insurance = float(st.get("insurance") or 0)
    ins_burden = insurance / home_value * 100
    pop_growth = float(st.get("pop_growth") or 0)
    migration = float(st.get("net_migration") or 0)
    permits = float(st.get("permits_per_1k") or 0)
    hv_yoy = float(st.get("home_value_yoy") or 0)

    flags = []
    if insurance >= 3500 or ins_burden >= 1.0:
        flags.append("insurance_shock")
    if pop_growth <= 0.0:
        flags.append("pop_decline")
    if migration <= -5.0:
        flags.append("out_migration")
    if permits >= 5.5 and hv_yoy <= 0.0:
        flags.append("supply_glut")
    if hv_yoy <= -2.0:
        flags.append("values_falling")

    return {
        "flags": flags,
        "chips": [{"key": f, "label": FLAG_META[f][0], "title": FLAG_META[f][1]}
                  for f in flags],
        "insurance": insurance,
        "insurance_burden_pct": round(ins_burden, 2),
        "pop_growth": pop_growth,
        "net_migration": migration,
        "permits_per_1k": permits,
        "home_value_yoy": hv_yoy,
    }


# ── Trajectory veto ──────────────────────────────────────────────────

def apply_trajectory_veto(score: float, traj_label: str | None,
                          n_state_flags: int = 0) -> tuple[float, bool]:
    """Let a bad trajectory cap a good level score, visibly.

      declining                → capped at 69 (79 if the state is clean)
      declining + ≥2 flags     → capped at 59
      decelerating             → −5 points

    Returns (adjusted_score, was_vetoed)."""
    if traj_label == "declining":
        cap = 59 if n_state_flags >= 2 else (69 if n_state_flags >= 1 else 79)
        if score > cap:
            return round(float(cap), 1), True
        return round(score, 1), False
    if traj_label == "decelerating":
        return round(max(0, score - 5), 1), True
    return round(score, 1), False


# ── State-level trajectory aggregates (for /map) ─────────────────────

@lru_cache(maxsize=1)
def state_trajectories() -> dict[str, dict]:
    """Median ZIP-level trajectory per state, computed once per process
    from zips.db (25k histories ≈ a second or two, then cached). Used
    by /map, where metros need a trend read but per-ZIP granularity
    would be overkill. Returns {} if the db is missing so callers can
    degrade to no-trajectory."""
    if not _ZIPS_DB.exists():
        return {}
    out: dict[str, dict] = {}
    try:
        conn = sqlite3.connect(str(_ZIPS_DB))
        rows = conn.execute(
            "SELECT state, history_zhvi FROM zips "
            "WHERE history_zhvi IS NOT NULL AND state IS NOT NULL"
        ).fetchall()
        conn.close()
    except sqlite3.Error as e:
        logger.warning("state_trajectories: zips.db read failed: %s", e)
        return {}

    by_state: dict[str, list[dict]] = {}
    for state, hist_json in rows:
        try:
            traj = trajectory_from_history(json.loads(hist_json))
        except (ValueError, TypeError):
            continue
        if traj:
            by_state.setdefault(state, []).append(traj)

    for state, trajs in by_state.items():
        if len(trajs) < 3:
            continue
        cagr = statistics.median(t["cagr_3yr_pct"] for t in trajs)
        chg1 = statistics.median(t["chg_1yr_pct"] for t in trajs)
        decel = statistics.median(t["decel_pct"] for t in trajs)
        if cagr < 0 and chg1 <= 0:
            label = "declining"
        elif decel <= DECEL_THRESHOLD and chg1 < cagr:
            label = "decelerating"
        elif decel >= ACCEL_THRESHOLD:
            label = "accelerating"
        else:
            label = "steady"
        out[state] = {
            "cagr_3yr_pct": round(cagr, 1),
            "chg_1yr_pct":  round(chg1, 1),
            "decel_pct":    round(decel, 1),
            "label":        label,
            "n_zips":       len(trajs),
        }
    return out


def _states() -> dict:
    """Late import so this module stays importable in isolation.
    CHOROPLETH_STATES is the rich per-state dict (insurance, pop_growth,
    permits, migration, …) — data_providers.STATES is just the metro
    registry and has none of those fields."""
    from data_providers import CHOROPLETH_STATES
    return CHOROPLETH_STATES

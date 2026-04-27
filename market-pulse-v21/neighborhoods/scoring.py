"""Sub-score functions + persona-weighted composites.

Generalized from dallas_neighborhoods.py. Persona weights are
re-normalized on the fly when a dimension is missing, so a county with
no Walk Score data yet doesn't artificially deflate its lifestyle
composite — the remaining weights scale up to fill the gap.
"""
from __future__ import annotations


# ───── Sub-score helpers (each returns 0-100, or None if input missing) ─────

def _score_cap_rate(cap_rate_pct):
    if cap_rate_pct is None:
        return None
    if cap_rate_pct <= 3.0:
        return 0.0
    if cap_rate_pct >= 8.0:
        return 100.0
    return (cap_rate_pct - 3.0) / 5.0 * 100.0


def _score_crime_safety(crime_index):
    if crime_index is None:
        return None
    return max(0.0, min(100.0, 100.0 - float(crime_index)))


def _score_schools(pct_bachelors):
    if pct_bachelors is None:
        return None
    if pct_bachelors <= 20:
        return 0.0
    if pct_bachelors >= 70:
        return 100.0
    return (pct_bachelors - 20) / 50.0 * 100.0


def _score_income(median_hh_income):
    if median_hh_income is None:
        return None
    if median_hh_income <= 40_000:
        return 0.0
    if median_hh_income >= 130_000:
        return 100.0
    return (median_hh_income - 40_000) / 90_000.0 * 100.0


def _score_affordability(median_home_value):
    if median_home_value is None:
        return None
    if median_home_value <= 200_000:
        return 100.0
    if median_home_value >= 1_000_000:
        return 0.0
    return (1_000_000 - median_home_value) / 800_000.0 * 100.0


def _score_walkability(walk_score):
    if walk_score is None:
        return None
    if walk_score <= 25:
        return 0.0
    if walk_score >= 90:
        return 100.0
    return (walk_score - 25) / 65.0 * 100.0


def _score_restaurants(restaurant_score):
    if restaurant_score is None:
        return None
    return max(0.0, min(100.0, float(restaurant_score)))


# ───── Personas ─────

PERSONAS: dict[str, dict] = {
    "investor": {
        "label": "💰 Investor",
        "description": "Cash flow first. Cap rate dominates; lifestyle features only matter to the extent they widen the renter pool.",
        "weights": {
            "cap_rate":      0.35,
            "crime_safety":  0.18,
            "schools":       0.15,
            "income":        0.12,
            "affordability": 0.10,
            "walkability":   0.05,
            "restaurants":   0.05,
        },
    },
    "lifestyle": {
        "label": "🍷 Lifestyle Buyer",
        "description": "Buying to live in. Walkability + restaurants + schools + low crime dominate; cap rate is a tiebreaker.",
        "weights": {
            "cap_rate":      0.05,
            "crime_safety":  0.20,
            "schools":       0.18,
            "income":        0.07,
            "affordability": 0.10,
            "walkability":   0.20,
            "restaurants":   0.20,
        },
    },
    "balanced": {
        "label": "⚖️ Balanced",
        "description": "Hybrid — cap rate matters but lifestyle pulls equal weight. Useful for owner-occupants who'll rent later.",
        "weights": {
            "cap_rate":      0.20,
            "crime_safety":  0.18,
            "schools":       0.16,
            "income":        0.10,
            "affordability": 0.10,
            "walkability":   0.13,
            "restaurants":   0.13,
        },
    },
}

DEFAULT_PERSONA = "investor"


def _composite_with_renormalization(sub_scores: dict, weights: dict) -> float:
    """Weighted sum, but re-normalize across only the dimensions that
    actually have data. This prevents a missing Walk Score from silently
    dragging a county's lifestyle score down before the API key is wired.
    """
    available_weight = 0.0
    weighted_total = 0.0
    for k, w in weights.items():
        v = sub_scores.get(k)
        if v is None:
            continue
        available_weight += w
        weighted_total += v * w
    if available_weight <= 0:
        return 0.0
    # Scale up so weights effectively sum to 1.0 over the available dims.
    return weighted_total / available_weight


def compute_zip_metrics(z: dict) -> dict:
    """Compute derived metrics + sub-scores + per-persona composites for one ZIP.

    Tolerant of missing fields — any sub-score whose source data is
    absent will be None, and the composite re-normalizes around what's
    present.
    """
    home_value = z.get("median_home_value")
    monthly_rent = z.get("median_rent_monthly")
    cap_rate_pct = None
    rent_to_price = None
    if home_value and monthly_rent and home_value > 0:
        annual_rent = monthly_rent * 12
        cap_rate_pct = annual_rent / home_value * 100
        rent_to_price = annual_rent / home_value

    sub_scores = {
        "cap_rate":      _score_cap_rate(cap_rate_pct),
        "crime_safety":  _score_crime_safety(z.get("crime_index")),
        "schools":       _score_schools(z.get("pct_bachelors")),
        "income":        _score_income(z.get("median_household_income")),
        "affordability": _score_affordability(home_value),
        "walkability":   _score_walkability(z.get("walk_score")),
        "restaurants":   _score_restaurants(z.get("restaurant_score")),
    }
    composite_by_persona = {
        name: round(_composite_with_renormalization(sub_scores, p["weights"]), 1)
        for name, p in PERSONAS.items()
    }
    # Frontend renders missing sub-scores as 0 in the bar, so coerce None → 0
    # in the dict it sees, but track which were actually missing for the UI.
    missing_dims = [k for k, v in sub_scores.items() if v is None]
    return {
        "cap_rate_pct": round(cap_rate_pct, 2) if cap_rate_pct is not None else None,
        "rent_to_price": round(rent_to_price, 4) if rent_to_price is not None else None,
        "sub_scores": {k: (round(v, 1) if v is not None else None) for k, v in sub_scores.items()},
        "missing_dimensions": missing_dims,
        "composite_by_persona": composite_by_persona,
        "composite_score": composite_by_persona[DEFAULT_PERSONA],
    }

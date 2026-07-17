"""Compounders screen — /compounders. The 14%/yr, 10-15 year test.

Long-run return decomposes into four terms and nothing else:

    E[return] ≈ organic growth + buyback yield + dividend yield
                ± valuation drift

so the page shows that math per stock instead of a black-box score.
The engine behind the growth term is ROIC × reinvestment — and the key
empirical fact shaping the gates: TRAILING GROWTH DOES NOT PERSIST,
HIGH ROIC DOES. We gate hard on the persistent things (return on
capital, cash conversion, balance sheet) and haircut the transient one
(growth) inside the expected-return math.

Hard gates (quality — all must pass):
    ROIC        10-yr median ≥ 15%
    Growth      blended revenue CAGR ≥ 4% AND up in ≥60% of years
    Profits     net income positive in ≥8 of 10 years
    Cash real   ΣFCF / Σnet income ≥ 70% over 10 yrs (fraud guard —
                reported profits must show up as cash)
    Debt        net debt / EBIT ≤ 3×
    Capex       capex / OCF ≤ 40% (capital-light)
    Country     not uninvestable (sanctioned / expropriation-tier)

Cyclicals (user decision: mid-cycle handling, never exclusion):
    A cyclical at ≥75th-percentile margins vs its own history can never
    be COMPOUNDER — it shows CYCLE-WAIT (buying peak margins is how
    cheap-looking cyclicals lose you money). A cyclical at ≤35th
    percentile with intact gates gets a "cycle low" highlight.

Statuses: COMPOUNDER (≥14% expected, gates pass) · QUALITY (10-14) ·
CYCLE-WAIT · WATCH (<10, usually the price) · GATED.

China included with warnings (user decision), Taiwan flagged for
geopolitics, Russia-tier excluded. Data: data/compounders.json built
monthly by scripts/refresh_compounders.py (EDGAR XBRL + Yahoo).
"""
from __future__ import annotations

import json
from pathlib import Path

_DATA_PATH = Path(__file__).resolve().parent / "data" / "compounders.json"

# ── Thresholds (tune here — no refetch needed) ───────────────────────
ROIC_MIN = 15.0
GROWTH_MIN = 4.0
UP_YEARS_FRAC = 0.60
NI_POS_MIN = 8
FCF_CONV_MIN = 70.0
ND_EBIT_MAX = 3.0
CAPEX_OCF_MAX = 40.0
TARGET = 14.0
QUALITY_FLOOR = 10.0
CYCLE_PEAK = 75
CYCLE_LOW = 35
GROWTH_HAIRCUT = 0.70          # trailing growth fades; don't pay full freight
MULT_DRIFT_CAP = 3.0           # valuation drift capped at ±3%/yr over 10y


def _load() -> dict:
    """data/compounders.json if present and well-formed, else {}."""
    try:
        with open(_DATA_PATH, encoding="utf-8") as fh:
            data = json.load(fh)
        if isinstance(data, dict) and isinstance(data.get("tickers"), dict):
            return data
    except (OSError, ValueError):
        pass
    return {}


def data_source_label() -> str:
    d = _load()
    if d:
        return f"EDGAR + Yahoo — {d.get('as_of', 'latest')} · {d.get('count', 0)} companies"
    return "no data yet — run the refresh workflow"


# ── Country handling ─────────────────────────────────────────────────
# EDGAR gives a free-text HQ country description. Everything not listed
# is treated as investable with no badge (the US default).

_CN = {"china", "hong kong"}
_TW = {"taiwan"}
_EXCLUDED = {"russia", "russian federation", "belarus"}
_FX_WARN = {"turkey", "türkiye", "argentina", "egypt", "nigeria", "pakistan"}


def _country_assess(desc: str | None) -> tuple[bool, list[dict]]:
    """(investable, badges) from the EDGAR country description."""
    d = (desc or "united states").strip().lower()
    badges = []
    if any(k in d for k in _EXCLUDED):
        return False, [{"key": "excluded", "label": "uninvestable",
                        "title": "Sanctioned / expropriation-tier jurisdiction — excluded regardless of quality."}]
    if any(k in d for k in _CN):
        badges.append({"key": "cn", "label": "⚠ China", "title":
                       "China/HK risk: VIE structures, audit-verification limits, delisting and policy risk. "
                       "Included per your call — size accordingly; fraud-gate metrics are less verifiable here."})
    if any(k in d for k in _TW):
        badges.append({"key": "tw", "label": "⚠ Taiwan", "title":
                       "Geopolitical tail risk (strait scenario). Quality can be world-class; the tail is real and unhedgeable."})
    if any(k in d for k in _FX_WARN):
        badges.append({"key": "fx", "label": "⚠ FX", "title":
                       "Chronically depreciating currency — local results may not reach USD returns."})
    return True, badges


# ── Scoring ──────────────────────────────────────────────────────────

def _blend_growth(r5, r10):
    vals = [v for v in (r5, r10) if v is not None]
    if not vals:
        return None
    return sum(vals) / len(vals)


def score() -> list[dict]:
    data = _load()
    rows = []
    for ticker, m in (data.get("tickers") or {}).items():
        if not isinstance(m, dict):
            continue
        investable, badges = _country_assess(m.get("country"))

        # ── Gates ──
        growth = _blend_growth(m.get("rev_cagr5"), m.get("rev_cagr10"))
        up, tot = m.get("rev_up_years") or 0, m.get("rev_up_total") or 0
        gates = {
            "roic":   m.get("roic_med") is not None and m["roic_med"] >= ROIC_MIN,
            "growth": growth is not None and growth >= GROWTH_MIN
                      and tot >= 6 and up / max(tot, 1) >= UP_YEARS_FRAC,
            "profit": (m.get("ni_pos_years") or 0) >= NI_POS_MIN,
            "cash":   m.get("fcf_conv") is not None and m["fcf_conv"] >= FCF_CONV_MIN,
            "debt":   m.get("nd_ebit") is not None and m["nd_ebit"] <= ND_EBIT_MAX,
            "capex":  m.get("capex_ocf") is not None and m["capex_ocf"] <= CAPEX_OCF_MAX,
            "country": investable,
        }
        gates_pass = all(gates.values())

        # ── Expected-return decomposition ──
        g = None
        if growth is not None:
            g = GROWTH_HAIRCUT * max(-2.0, min(14.0, growth))
            slope, om = m.get("margin_slope"), m.get("op_margin_now")
            if slope is not None and om and om > 0:
                g += max(-1.0, min(1.5, slope / om * 100))
        b = 0.0
        if m.get("shares_cagr5") is not None:
            b = max(-3.0, min(4.0, -m["shares_cagr5"]))
        d_yield = m.get("div_yield") or 0.0
        mult = None
        rich = False
        pf_now, pf_med = m.get("pfcf_now"), m.get("pfcf_med")
        if pf_now and pf_med and pf_now > 0:
            mult = ((pf_med / pf_now) ** 0.1 - 1) * 100
            mult = max(-MULT_DRIFT_CAP, min(MULT_DRIFT_CAP, mult))
            rich = mult <= -2.0
        elif m.get("fcf_last") is not None and m["fcf_last"] <= 0:
            mult = -2.0    # negative FCF today: pay a drift penalty + flag

        expected = None
        if g is not None:
            expected = round(g + b + d_yield + (mult or 0.0), 1)

        # ── Cyclicality ──
        cyclical = bool(m.get("cyclical"))
        cycle_pos = m.get("cycle_pos")
        cycle_wait = cyclical and cycle_pos is not None and cycle_pos >= CYCLE_PEAK
        cycle_low = cyclical and cycle_pos is not None and cycle_pos <= CYCLE_LOW
        if cycle_wait:
            badges.append({"key": "cycpeak", "label": "cycle peak", "title":
                           f"Cyclical at the {cycle_pos}th percentile of its own 10-yr margins — earnings and the multiple both look best right before they don't. Wait."})
        elif cycle_low and gates_pass:
            badges.append({"key": "cyclow", "label": "cycle low", "title":
                           f"Cyclical at the {cycle_pos}th percentile of its own margin history with the quality engine intact — the buy-the-low setup."})
        if rich:
            badges.append({"key": "rich", "label": "rich", "title":
                           "Trading well above its own 10-yr median P/FCF — the valuation-drift term is eating your return."})

        # ── Status ──
        if not gates_pass:
            status = "GATED"
        elif cycle_wait:
            status = "CYCLE-WAIT"
        elif expected is not None and expected >= TARGET:
            status = "COMPOUNDER"
        elif expected is not None and expected >= QUALITY_FLOOR:
            status = "QUALITY"
        else:
            status = "WATCH"

        rows.append({
            "ticker": ticker, **{k: m.get(k) for k in (
                "name", "country", "industry", "exchange", "fy_last", "years",
                "revenue_last", "rev_cagr5", "rev_cagr10", "roic_med",
                "fcf_conv", "nd_ebit", "capex_ocf", "shares_cagr5",
                "gross_margin", "op_margin_now", "op_margin_med",
                "cycle_pos", "cyclical", "div_yield", "div_cagr5",
                "pfcf_now", "pfcf_med", "price")},
            "growth_blend": round(growth, 1) if growth is not None else None,
            "er_growth": round(g, 1) if g is not None else None,
            "er_buyback": round(b, 1),
            "er_div": round(d_yield, 1),
            "er_mult": round(mult, 1) if mult is not None else 0.0,
            "expected": expected,
            "gates": gates,
            "gates_pass": gates_pass,
            "badges": badges,
            "status": status,
        })

    order = {"COMPOUNDER": 0, "QUALITY": 1, "CYCLE-WAIT": 2, "WATCH": 3, "GATED": 4}
    rows.sort(key=lambda r: (order[r["status"]],
                             -(r["expected"] if r["expected"] is not None else -99)))
    return rows


def summary(rows: list[dict] | None = None) -> dict:
    rows = rows if rows is not None else score()
    from collections import Counter
    counts = Counter(r["status"] for r in rows)
    return {"total": len(rows), **{k.lower().replace("-", "_"): v for k, v in counts.items()}}

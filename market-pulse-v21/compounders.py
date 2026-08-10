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

# Cash conversion is FCF over net income, and net income is the one term
# in it that can approach zero. News Corp came through at 27,815%, Helix
# at 16,355%, Koss at 7,659% — 21 rows above 1,000%. None of those is a
# company converting profits to cash three hundred times over; each is a
# company whose cumulative earnings were close to nothing, so the ratio
# is dividing by noise and swings wildly on a rounding error.
#
# Above this level the number stops discriminating: 300% and 27,815% both
# say the same thing — cash comfortably exceeds accounting profit — and
# the gate only asks for FCF_CONV_MIN. The raw figure is kept and the row
# is badged, exactly as the dividend cap works.
FCF_CONV_CAP = 300.0
TARGET = 14.0
QUALITY_FLOOR = 10.0
CYCLE_PEAK = 75
CYCLE_LOW = 35
GROWTH_HAIRCUT = 0.70          # trailing growth fades; don't pay full freight
MULT_DRIFT_CAP = 3.0           # valuation drift capped at ±3%/yr over 10y

# ── The dividend term ────────────────────────────────────────────────
#
# Every other term here is bounded, because a trailing figure is not a
# forecast: growth is haircut and clamped to 14, buybacks to ±4, drift to
# ±3. The dividend went in RAW — and it is the one input that grows as the
# market loses confidence, because it divides last year's payout by a
# collapsed price. It was also the only term with no upper bound at all.
#
# What that produced, on the live board:
#
#     SDEV  Stablecoin Development   E[r] 400.2   D = 388.4
#     AD    Array Digital Infra      E[r] 121.1   D = 125.6
#     VISN  Vistance Networks        E[r]  84.0   D =  91.4
#     SSTK  Shutterstock             E[r]  35.4   D =  23.1
#
# A 388% dividend yield is a special distribution, a data error, or a
# company liquidating itself. It is not a forecast of a 388% annual
# return, and the model read it as one.
#
# TWO INDEPENDENT TESTS, because they fail for different reasons.
#
#   COVERAGE — is the cash there? Payout against free cash flow falls out
#   of what is already stored: yield x P/FCF. Above 100%, the dividend is
#   being funded from the balance sheet, and the most it can be worth is
#   the cash the business actually generates — so it is capped at the FCF
#   yield.
#
#   LEVEL — even a covered yield this high is the market pricing a cut.
#   The base rate of a double-digit yield surviving is poor, and a model
#   should not silently out-forecast the market on the strength of one
#   trailing number. This is a PRIOR, not a measurement, which is why it
#   sits here with the other tunables and is shown on every row it
#   touches rather than applied quietly.
DIV_YIELD_CAP = 8.0
DIV_PAYOUT_MAX = 100.0         # % of free cash flow

# ── Evidence, not exclusion ──────────────────────────────────────────
#
# COMPOUNDER is a claim about fifteen years. A company with eight years of
# filings has not earned or failed that claim — it has not been asked the
# question. So the label requires the record, and everything shorter is
# still shown, still scored, and marked SHORT HISTORY.
#
# A young company is not a fraud, and hiding it means never seeing it. But
# a decade-and-a-half claim resting on eight years is the screen agreeing
# with itself, and the three names it currently affects — SSTK, COLM, AOS
# at 8, 8 and 7 years — are the same three whose growth rested entirely on
# a COVID-distorted five-year number with no ten-year figure to check it
# against. That convergence is not a coincidence.
LONG_HISTORY_YEARS = 15

# ── Plausibility, same treatment as the dividend ─────────────────────
# Every one of these produced a real number from real filings and stopped
# describing the business at the extreme. P/FCF ranges from 0.1 to 34,400
# on the live board; ROIC reaches 179.7% when invested capital rounds to
# nothing. Outside these bands the input is not measuring what its name
# says, so the term it feeds is withheld rather than trusted.
PFCF_SANE = (2.0, 200.0)
ROIC_SANE_MAX = 100.0


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
# EDGAR gives a free-text HQ country description, and for roughly half of
# foreign private issuers it gives nothing at all.

_CN = {"china", "hong kong"}
_TW = {"taiwan"}
_EXCLUDED = {"russia", "russian federation", "belarus"}
_FX_WARN = {"turkey", "türkiye", "argentina", "egypt", "nigeria", "pakistan"}

# Places companies are registered but do not operate. Reading one of these
# as the country would put Alibaba in the Caribbean. They are reported as
# what they are — a domicile — and the operating country stays unknown.
_OFFSHORE = {
    "cayman islands", "british virgin islands", "virgin islands, british",
    "bermuda", "marshall islands", "jersey", "guernsey", "isle of man",
    "bahamas", "panama", "curacao", "netherlands antilles", "gibraltar",
}

_US_LITERAL = "united states"

# EDGAR gives domestic filers a two-letter state. A 20-F/40-F filer that
# shows one is showing its US agent's office, not where it is: Shell came
# through as "DC". Canadian provinces arrive as "Ontario, Canada" and are
# longer than two characters, so they never collide with this.
_US_STATES = frozenset(
    "AL AK AZ AR CA CO CT DE DC FL GA HI ID IL IN IA KS KY LA ME MD MA MI "
    "MN MS MO MT NE NV NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT "
    "VA WA WV WI WY PR VI GU AS MP".split())


def resolve_location(country: str | None = None, incorporation: str | None = None,
                     foreign: bool = False) -> dict:
    """What the Location column should say, and whether we actually know it.

    THE BUG THIS REPLACES. The build wrote the literal "United States"
    whenever EDGAR's business-address country description came back empty,
    which it does for about half of foreign private issuers. On the last
    board that was 160 of 339 20-F/40-F filers — Alibaba, Baidu, NetEase,
    Weibo, PDD, TSMC, Sony and Infosys among them — printed as US
    companies. The screen looked like it had no international coverage
    when 17% of it was international, and worse, the China badge fired on
    15 rows instead of the forty-odd that warrant one, because the badge
    reads the country field that had just been overwritten.

    A company filing 20-F or 40-F is BY DEFINITION not a US domestic
    issuer. "Foreign filer, United States" is self-contradictory, so it is
    treated here as the absence of an answer rather than as an answer —
    the same rule the rest of the app runs on.

    Returns {label, known, basis, domicile_only}.
    """
    c = (country or "").strip()
    inc = (incorporation or "").strip()

    # Self-contradictory: discard rather than trust. Covers both the
    # manufactured "United States" and the US agent's office that Shell,
    # AerCap and Allegion all show — Shell arrives with the address
    # "United States" AND the incorporation "DC", so both fields need it.
    if foreign:
        if c.lower() == _US_LITERAL or c.upper() in _US_STATES:
            c = ""
        if inc.lower() == _US_LITERAL or inc.upper() in _US_STATES:
            inc = ""
    if c:
        return {"label": c, "known": True, "basis": "address",
                "domicile_only": False}
    if inc:
        off = inc.lower() in _OFFSHORE
        return {"label": inc, "known": not off, "basis": "incorporation",
                "domicile_only": off}
    if foreign:
        return {"label": "Foreign filer", "known": False, "basis": "form",
                "domicile_only": False}
    return {"label": "", "known": False, "basis": None, "domicile_only": False}


def _country_assess(desc: str | None, known: bool = True,
                    domicile_only: bool = False) -> tuple[bool, list[dict]]:
    """(investable, badges) from the resolved location.

    An unknown origin no longer defaults to the United States. That
    default did not merely mislabel a column — it silently cleared the
    sanctions check and suppressed the China and Taiwan warnings for every
    company whose country we failed to read, which is precisely the
    population those warnings exist for.

    Unknown stays investable, because a company we could not place is not
    a company we have grounds to reject, but it is badged so the reader
    knows the jurisdiction screen did not run rather than assuming it
    passed.
    """
    d = (desc or "").strip().lower()
    badges = []
    if any(k in d for k in _EXCLUDED):
        return False, [{"key": "excluded", "label": "uninvestable",
                        "title": "Sanctioned / expropriation-tier jurisdiction — excluded regardless of quality."}]
    if domicile_only:
        badges.append({"key": "domicile", "label": "domicile only", "title":
                       f"Registered in {desc}, which is a domicile and not an operating "
                       "location. The country the business actually runs in is not "
                       "established, so the China/Taiwan/FX checks could not be applied. "
                       "Offshore registration is itself the signature of a VIE or "
                       "similar structure — read the 20-F before sizing."})
    elif not known:
        badges.append({"key": "origin", "label": "origin unverified", "title":
                       "EDGAR gave no usable country for this filer, so the jurisdiction "
                       "screen (sanctions, China/HK, Taiwan, FX) did not run. Not a "
                       "finding against the company — a gap in what we could read."})
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

def _blend_growth(r5, r10, trend=None):
    """The growth figure the gate and the E[r] term both use.

    The FITTED fifteen-year trend wins when it exists, because it rests on
    every year rather than on two endpoints. The endpoint CAGRs remain the
    fallback for companies too young to fit, and both are still displayed
    so a divergence between them is visible rather than averaged away.
    """
    if trend is not None:
        return trend
    vals = [v for v in (r5, r10) if v is not None]
    if not vals:
        return None
    return sum(vals) / len(vals)


def score(data: dict | None = None) -> list[dict]:
    """Score every company. `data` is injectable so the maths can be
    tested without a 2MB fixture on disk."""
    data = _load() if data is None else data
    rows = []
    for ticker, m in (data.get("tickers") or {}).items():
        if not isinstance(m, dict):
            continue
        loc = resolve_location(m.get("country"), m.get("incorporation"),
                               bool(m.get("foreign")))
        investable, badges = _country_assess(loc["label"], loc["known"],
                                             loc["domicile_only"])

        # ── Gates ──
        # TOTAL reinvestment, not just PP&E. Comcast spends ~$2.8bn a year
        # on capitalised intangibles on top of $12.5bn of capex; reading
        # the PP&E line alone understates what a cable, media or software
        # business must spend to stand still. `reinvest_ocf` is absent from
        # boards built before this landed, so capex/OCF remains the
        # fallback — the gate then measures less, and says so in a badge
        # rather than pretending the two are the same number.
        # Bounded before anything reads it. The cap sits far above
        # FCF_CONV_MIN, so no company's gate result changes — this makes
        # the number honest, not the screen stricter.
        conv_raw = m.get("fcf_conv")
        conv = min(conv_raw, FCF_CONV_CAP) if conv_raw is not None else None
        conv_capped = conv_raw is not None and conv_raw > FCF_CONV_CAP

        reinvest = m.get("reinvest_ocf")
        if reinvest is None:
            reinvest = m.get("capex_ocf")
        # EXACTLY ZERO IS NOT A MEASUREMENT. No operating company spends
        # literally nothing on plant across five consecutive years, and
        # 162 rows on the last board did — the signature of the build
        # reading an untagged capex year as a zero-capex year. The build is
        # fixed, but boards written before that carry the zeros, so they
        # are refused here too rather than clearing the gate at 0%.
        if reinvest is not None and reinvest <= 0.0:
            reinvest = None
        growth = _blend_growth(m.get("rev_cagr5"), m.get("rev_cagr10"),
                               m.get("rev_trend"))
        up, tot = m.get("rev_up_years") or 0, m.get("rev_up_total") or 0
        gates = {
            "roic":   m.get("roic_med") is not None and m["roic_med"] >= ROIC_MIN,
            "growth": growth is not None and growth >= GROWTH_MIN
                      and tot >= 6 and up / max(tot, 1) >= UP_YEARS_FRAC,
            "profit": (m.get("ni_pos_years") or 0) >= NI_POS_MIN,
            "cash":   conv is not None and conv >= FCF_CONV_MIN,
            "debt":   m.get("nd_ebit") is not None and m["nd_ebit"] <= ND_EBIT_MAX,
            "capex":  reinvest is not None and reinvest <= CAPEX_OCF_MAX,
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
        mult = None
        rich = False
        pf_now, pf_med = m.get("pfcf_now"), m.get("pfcf_med")
        lo, hi = PFCF_SANE
        if pf_now is not None and not (lo <= pf_now <= hi):
            # NVMI reads 0.1 against a $393 share price, which implies a
            # share count two orders of magnitude out. Rewarding that with
            # a full +3.0 re-rating term is the model paying for a bug.
            badges.append({"key": "pfcfodd", "label": "P/FCF suspect", "title":
                           f"P/FCF of {pf_now} is outside {lo:g}-{hi:g}x. The per-share "
                           f"arithmetic behind it cannot be right, so the valuation term "
                           f"is withheld rather than computed from it."})
            pf_now = None
        if pf_now and pf_med and pf_now > 0:
            mult = ((pf_med / pf_now) ** 0.1 - 1) * 100
            mult = max(-MULT_DRIFT_CAP, min(MULT_DRIFT_CAP, mult))
            rich = mult <= -2.0
        elif m.get("fcf_last") is not None and m["fcf_last"] <= 0:
            mult = -2.0    # negative FCF today: pay a drift penalty + flag

        # ── Dividend: bounded like every other term ──
        d_raw = m.get("div_yield") or 0.0
        d_yield, payout, d_note = d_raw, None, None
        if d_raw > 0 and pf_now and pf_now > 0:
            # yield% x P/FCF = dividends as a percentage of free cash flow.
            payout = round(d_raw * pf_now, 1)
            if payout > DIV_PAYOUT_MAX:
                funded = 100.0 / pf_now          # the FCF yield
                if funded < d_yield:
                    d_yield = funded
                    d_note = (f"pays {payout:.0f}% of free cash flow — the part "
                              f"beyond {funded:.1f}% comes off the balance sheet, "
                              f"not out of the business")
        if d_yield > DIV_YIELD_CAP:
            d_note = (f"{d_raw:.1f}% trailing yield counted as {DIV_YIELD_CAP:.0f}% — "
                      f"at that level the market is pricing a cut, and a trailing "
                      f"payout over a fallen price is not a forecast")
            d_yield = DIV_YIELD_CAP
        if d_note:
            badges.append({"key": "divcap", "label": "div capped", "title": d_note})

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

        # ── Reinvestment we could not measure ──
        # 162 rows on the last board reported capex/OCF as exactly 0.0% —
        # Shell, BP, Verizon, Rio Tinto, Toyota — because an untagged capex
        # year was being read as a zero-capex year. They now score None and
        # fail this gate, which is the honest answer, but a row that says
        # nothing about why looks identical to a company that genuinely
        # flunked on heavy spending. This badge is the difference.
        if conv_capped:
            badges.append({"key": "convcap", "label": "conv capped", "title":
                           f"Cash conversion computes to {conv_raw:,.0f}% of net income and is "
                           f"shown at {FCF_CONV_CAP:.0f}%. That is not three hundred times better "
                           f"conversion — it is a company whose cumulative earnings were near "
                           f"zero, so the ratio divides by noise and moves wildly on a rounding "
                           f"error. The gate only asks for {FCF_CONV_MIN:.0f}%, so nothing about "
                           f"this company's result changed."})

        if reinvest is None:
            badges.append({"key": "nocapex", "label": "capex unfiled", "title":
                           "No capital-expenditure figure could be found in at least three "
                           "of the last five years, so reinvestment — and therefore free "
                           "cash flow — could not be measured. The company is GATED "
                           "because we could not verify it, not because it failed. "
                           "Common for IFRS filers and extractive industries, whose cash "
                           "flow statements use tags this build may not yet read."})
        elif m.get("reinvest_ocf") is None:
            badges.append({"key": "capexppe", "label": "PP&E only", "title":
                           "Reinvestment here counts property, plant and equipment only. "
                           "Capitalised software, content and spectrum are real spending "
                           "that never touches that line, so for media, cable and software "
                           "businesses this figure is a floor. Rebuild the board to include "
                           "it."})

        # ── Evidence length ──
        yrs = m.get("years") or 0
        proven = yrs >= LONG_HISTORY_YEARS
        if not proven and gates_pass:
            badges.append({"key": "short", "label": f"{yrs}y history", "title":
                           f"Only {yrs} fiscal years on file — COMPOUNDER is a claim about "
                           f"{LONG_HISTORY_YEARS} years, and this company has not been asked "
                           f"that question yet. It is scored and shown in full; it just "
                           f"cannot carry a label its record does not support."})

        # ── Status ──
        if not gates_pass:
            status = "GATED"
        elif cycle_wait:
            status = "CYCLE-WAIT"
        elif expected is not None and expected >= TARGET and proven:
            status = "COMPOUNDER"
        elif expected is not None and expected >= QUALITY_FLOOR:
            status = "QUALITY"
        elif expected is not None and expected >= TARGET:
            # Clears the bar on numbers but not on record: still QUALITY,
            # never silently demoted to WATCH.
            status = "QUALITY"
        else:
            status = "WATCH"

        rows.append({
            "ticker": ticker, **{k: m.get(k) for k in (
                "name", "country", "industry", "exchange", "fy_last", "years",
                "revenue_last", "rev_cagr5", "rev_cagr10", "roic_med",
                "fcf_conv", "nd_ebit", "capex_ocf", "reinvest_ocf", "shares_cagr5",
                "gross_margin", "op_margin_now", "op_margin_med",
                "cycle_pos", "cyclical", "div_yield", "div_cagr5",
                "pfcf_now", "pfcf_med", "price", "rev_cagr15", "rev_trend",
                "currency", "foreign")},
            # `country` above is the RAW build value, kept so a regression
            # is still diagnosable. `location` is what the page prints.
            "fcf_conv": conv,
            "fcf_conv_raw": conv_raw,
            "fcf_conv_capped": conv_capped,
            "location": loc["label"],
            "location_known": loc["known"],
            "location_basis": loc["basis"],
            "reinvest": reinvest,
            "growth_blend": round(growth, 1) if growth is not None else None,
            "er_growth": round(g, 1) if g is not None else None,
            "er_buyback": round(b, 1),
            "er_div": round(d_yield, 1),
            "div_yield_raw": round(d_raw, 2),
            "div_payout_fcf": payout,
            "div_capped": d_note is not None,
            "proven": proven,
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

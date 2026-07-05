"""Global country valuation + business-cycle data for the /global-values page.

Snapshot values are hand-curated from public sources. Refresh cadence:

  • CAPE, P/B, dividend yield → Damodaran quarterly file at
    https://pages.stern.nyu.edu/~adamodar/New_Home_Page/dataarchived.html
    (updated ~mid-January, mid-April, mid-July, mid-October)
  • Buffett indicator (market cap / GDP) → World Bank + FRED, quarterly
  • OECD CLI → https://data.oecd.org/leadind/composite-leading-indicator-cli.htm
    updated ~10th of every month
  • Percentiles reflect current metric vs the country's own 20-year range

All values are TICK-YOUR-OWN estimates as of the LAST_UPDATED date below.
Don't paper-trade off this without pulling fresh numbers first.
"""
from __future__ import annotations

LAST_UPDATED = "2026-Q1"


# Region tags used for coloring the quadrant chart and grouping in the table.
DEVELOPED = "Developed"
EMERGING  = "Emerging"


# Each country: name, code, region, single-country US-listed ETF ticker,
# current valuation metrics + their percentile (0-100) vs the country's
# own 20-year history (higher percentile = MORE expensive / less attractive),
# current OECD CLI value + 3-month direction ("rising" / "falling" / "flat").
#
# For DIVIDEND YIELD only: HIGH yield = cheap, so we store `div_yield_percentile`
# where HIGHER percentile = HIGHER yield = CHEAPER. In the composite calc we
# invert div_yield_percentile so all four metrics point the same way.
COUNTRIES = [
    # ── Developed markets ─────────────────────────────────────────
    dict(name="United States",   code="US", region=DEVELOPED, etf="SPY",  etf_name="SPDR S&P 500",
         cape=35.0, cape_pct=92, pb=4.6, pb_pct=88, div_yield=1.3, div_yield_pct=8,  buffett=195, buffett_pct=96,
         cli=100.2, cli_trend="flat"),
    dict(name="Japan",           code="JP", region=DEVELOPED, etf="EWJ",  etf_name="iShares MSCI Japan",
         cape=22.0, cape_pct=58, pb=1.5, pb_pct=45, div_yield=2.3, div_yield_pct=55, buffett=145, buffett_pct=72,
         cli=100.8, cli_trend="rising"),
    dict(name="United Kingdom",  code="UK", region=DEVELOPED, etf="EWU",  etf_name="iShares MSCI United Kingdom",
         cape=17.5, cape_pct=32, pb=1.9, pb_pct=38, div_yield=3.7, div_yield_pct=72, buffett=108, buffett_pct=45,
         cli=100.1, cli_trend="rising"),
    dict(name="Germany",         code="DE", region=DEVELOPED, etf="EWG",  etf_name="iShares MSCI Germany",
         cape=17.0, cape_pct=35, pb=1.7, pb_pct=42, div_yield=3.0, div_yield_pct=58, buffett=55,  buffett_pct=25,
         cli=99.4,  cli_trend="rising"),
    dict(name="France",          code="FR", region=DEVELOPED, etf="EWQ",  etf_name="iShares MSCI France",
         cape=19.5, cape_pct=48, pb=1.9, pb_pct=48, div_yield=3.2, div_yield_pct=62, buffett=110, buffett_pct=55,
         cli=99.6,  cli_trend="flat"),
    dict(name="Canada",          code="CA", region=DEVELOPED, etf="EWC",  etf_name="iShares MSCI Canada",
         cape=22.5, cape_pct=68, pb=2.2, pb_pct=62, div_yield=2.9, div_yield_pct=45, buffett=155, buffett_pct=75,
         cli=100.5, cli_trend="rising"),
    dict(name="Switzerland",     code="CH", region=DEVELOPED, etf="EWL",  etf_name="iShares MSCI Switzerland",
         cape=26.5, cape_pct=72, pb=3.4, pb_pct=68, div_yield=2.6, div_yield_pct=35, buffett=245, buffett_pct=88,
         cli=100.3, cli_trend="flat"),
    dict(name="Australia",       code="AU", region=DEVELOPED, etf="EWA",  etf_name="iShares MSCI Australia",
         cape=19.0, cape_pct=55, pb=2.2, pb_pct=58, div_yield=3.9, div_yield_pct=68, buffett=118, buffett_pct=48,
         cli=100.4, cli_trend="rising"),
    dict(name="Netherlands",     code="NL", region=DEVELOPED, etf="EWN",  etf_name="iShares MSCI Netherlands",
         cape=22.0, cape_pct=62, pb=2.5, pb_pct=60, div_yield=2.3, div_yield_pct=32, buffett=175, buffett_pct=78,
         cli=99.8,  cli_trend="rising"),
    dict(name="Sweden",          code="SE", region=DEVELOPED, etf="EWD",  etf_name="iShares MSCI Sweden",
         cape=24.0, cape_pct=68, pb=2.4, pb_pct=62, div_yield=3.1, div_yield_pct=52, buffett=145, buffett_pct=70,
         cli=100.2, cli_trend="rising"),
    dict(name="Spain",           code="ES", region=DEVELOPED, etf="EWP",  etf_name="iShares MSCI Spain",
         cape=14.0, cape_pct=18, pb=1.4, pb_pct=25, div_yield=4.2, div_yield_pct=78, buffett=88,  buffett_pct=42,
         cli=101.2, cli_trend="rising"),
    dict(name="Italy",           code="IT", region=DEVELOPED, etf="EWI",  etf_name="iShares MSCI Italy",
         cape=13.5, cape_pct=22, pb=1.3, pb_pct=30, div_yield=4.5, div_yield_pct=82, buffett=45,  buffett_pct=22,
         cli=100.5, cli_trend="rising"),
    dict(name="Hong Kong",       code="HK", region=DEVELOPED, etf="EWH",  etf_name="iShares MSCI Hong Kong",
         cape=10.0, cape_pct=8,  pb=1.0, pb_pct=12, div_yield=4.8, div_yield_pct=85, buffett=1050,buffett_pct=95,
         cli=99.5,  cli_trend="rising"),
    dict(name="Singapore",       code="SG", region=DEVELOPED, etf="EWS",  etf_name="iShares MSCI Singapore",
         cape=13.5, cape_pct=25, pb=1.3, pb_pct=28, div_yield=4.2, div_yield_pct=75, buffett=210, buffett_pct=85,
         cli=100.1, cli_trend="rising"),
    # ── Emerging markets ──────────────────────────────────────────
    dict(name="South Korea",     code="KR", region=EMERGING,  etf="EWY",  etf_name="iShares MSCI South Korea",
         cape=11.5, cape_pct=15, pb=1.1, pb_pct=18, div_yield=2.4, div_yield_pct=48, buffett=100, buffett_pct=42,
         cli=100.6, cli_trend="rising"),
    dict(name="Taiwan",          code="TW", region=EMERGING,  etf="EWT",  etf_name="iShares MSCI Taiwan",
         cape=28.0, cape_pct=82, pb=3.2, pb_pct=78, div_yield=2.8, div_yield_pct=45, buffett=210, buffett_pct=82,
         cli=101.0, cli_trend="rising"),
    dict(name="China",           code="CN", region=EMERGING,  etf="MCHI", etf_name="iShares MSCI China",
         cape=11.0, cape_pct=12, pb=1.4, pb_pct=15, div_yield=2.8, div_yield_pct=62, buffett=65,  buffett_pct=28,
         cli=99.2,  cli_trend="rising"),
    dict(name="India",           code="IN", region=EMERGING,  etf="INDA", etf_name="iShares MSCI India",
         cape=32.0, cape_pct=88, pb=3.9, pb_pct=85, div_yield=1.2, div_yield_pct=15, buffett=125, buffett_pct=78,
         cli=100.5, cli_trend="flat"),
    dict(name="Brazil",          code="BR", region=EMERGING,  etf="EWZ",  etf_name="iShares MSCI Brazil",
         cape=13.0, cape_pct=25, pb=1.5, pb_pct=32, div_yield=6.5, div_yield_pct=88, buffett=60,  buffett_pct=25,
         cli=99.8,  cli_trend="rising"),
    dict(name="Mexico",          code="MX", region=EMERGING,  etf="EWW",  etf_name="iShares MSCI Mexico",
         cape=15.0, cape_pct=42, pb=1.9, pb_pct=48, div_yield=3.5, div_yield_pct=65, buffett=35,  buffett_pct=18,
         cli=99.5,  cli_trend="rising"),
    dict(name="Thailand",        code="TH", region=EMERGING,  etf="THD",  etf_name="iShares MSCI Thailand",
         cape=16.0, cape_pct=45, pb=1.6, pb_pct=42, div_yield=3.5, div_yield_pct=62, buffett=95,  buffett_pct=52,
         cli=99.9,  cli_trend="rising"),
    dict(name="Indonesia",       code="ID", region=EMERGING,  etf="EIDO", etf_name="iShares MSCI Indonesia",
         cape=17.5, cape_pct=48, pb=2.1, pb_pct=52, div_yield=3.0, div_yield_pct=55, buffett=45,  buffett_pct=22,
         cli=100.6, cli_trend="rising"),
    dict(name="South Africa",    code="ZA", region=EMERGING,  etf="EZA",  etf_name="iShares MSCI South Africa",
         cape=14.0, cape_pct=28, pb=1.7, pb_pct=38, div_yield=4.2, div_yield_pct=72, buffett=280, buffett_pct=68,
         cli=99.7,  cli_trend="rising"),
    dict(name="Turkey",          code="TR", region=EMERGING,  etf="TUR",  etf_name="iShares MSCI Turkey",
         cape=10.0, cape_pct=15, pb=1.2, pb_pct=18, div_yield=3.5, div_yield_pct=58, buffett=40,  buffett_pct=32,
         cli=100.9, cli_trend="rising"),
    dict(name="Poland",          code="PL", region=EMERGING,  etf="EPOL", etf_name="iShares MSCI Poland",
         cape=10.5, cape_pct=18, pb=1.1, pb_pct=15, div_yield=4.0, div_yield_pct=68, buffett=32,  buffett_pct=18,
         cli=101.1, cli_trend="rising"),
    dict(name="Malaysia",        code="MY", region=EMERGING,  etf="EWM",  etf_name="iShares MSCI Malaysia",
         cape=14.5, cape_pct=32, pb=1.4, pb_pct=25, div_yield=3.6, div_yield_pct=62, buffett=110, buffett_pct=48,
         cli=99.6,  cli_trend="flat"),
    dict(name="Philippines",     code="PH", region=EMERGING,  etf="EPHE", etf_name="iShares MSCI Philippines",
         cape=13.0, cape_pct=25, pb=1.6, pb_pct=32, div_yield=2.4, div_yield_pct=42, buffett=75,  buffett_pct=38,
         cli=100.3, cli_trend="rising"),
    dict(name="Chile",           code="CL", region=EMERGING,  etf="ECH",  etf_name="iShares MSCI Chile",
         cape=14.0, cape_pct=32, pb=1.5, pb_pct=35, div_yield=4.5, div_yield_pct=75, buffett=85,  buffett_pct=42,
         cli=99.9,  cli_trend="rising"),
]


def composite_scores(countries: list[dict] | None = None) -> list[dict]:
    """Compute the two composite scores per country (0-100 scale, both
    'higher is better for buying'):

      • valuation_score   — 100 minus average of the 4 valuation-percentile
        metrics (CAPE, P/B, Buffett all point 'higher pct = expensive';
        div_yield is inverted so 'higher pct = cheap' aligns with the
        other three). 100 = cheapest of the group's own history.

      • cycle_score       — OECD CLI mapped to 0-100 around 100.0 as
        the anchor (CLI 100 = trend). Trend direction adds/subtracts a
        20-point bonus/penalty. 100 = strongly accelerating from below.

    Returns countries with the extra fields added, sorted by combined
    score (valuation + cycle) descending.
    """
    src = countries if countries is not None else COUNTRIES
    out = []
    for c in src:
        # Valuation: lower percentile = cheaper. For div_yield we invert.
        v = (c["cape_pct"] + c["pb_pct"] + (100 - c["div_yield_pct"]) + c["buffett_pct"]) / 4
        valuation_score = round(100 - v, 1)  # cheap = high score

        # Cycle: OECD CLI. 100.0 is the trend line. Above 100 = expansion.
        # We want "turning up" = the sweet spot, so bottom-out + rising
        # scores highest.
        cli = c["cli"]
        # Base score: how far from 100 (in the "rising" direction). CLI 98
        # rising toward 100 = mid-70s; CLI 102 rising = mid-40s (peaking).
        if c["cli_trend"] == "rising":
            base = 50 + (100 - cli) * 12   # 98→74, 100→50, 102→26
        elif c["cli_trend"] == "falling":
            base = 30 - (100 - cli) * 12   # 98→54 no wait, 102→54 → cap
        else:  # flat
            base = 50 - abs(100 - cli) * 8
        cycle_score = round(max(0, min(100, base)), 1)

        combined = round((valuation_score + cycle_score) / 2, 1)

        out.append({
            **c,
            "valuation_score": valuation_score,
            "cycle_score":     cycle_score,
            "combined_score":  combined,
            "quadrant":        _quadrant(valuation_score, cycle_score),
        })
    out.sort(key=lambda x: x["combined_score"], reverse=True)
    return out


def _quadrant(v: float, c: float) -> str:
    """Which of the 4 quadrants this country sits in. Cheap + expanding
    = BUY; cheap + contracting = WATCH; expensive + expanding =
    MOMENTUM; expensive + contracting = AVOID."""
    if v >= 50 and c >= 50: return "BUY"
    if v >= 50 and c <  50: return "WATCH"
    if v <  50 and c >= 50: return "MOMENTUM"
    return "AVOID"


def buy_list(scored: list[dict]) -> list[dict]:
    """The countries currently in the 'cheap + accelerating' BUY quadrant,
    ranked by combined score. Empty if no country meets the criteria."""
    return [c for c in scored if c["quadrant"] == "BUY"]

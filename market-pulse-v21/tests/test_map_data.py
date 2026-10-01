"""The national ZIP map's data (map_data.py) and page (templates/map.html)
on a small fixture profile: one basis per map, the same arithmetic as the
ZIP page, flags kept rather than dropped, every figure dated.

Run: python tests/test_map_data.py
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))

import build_zip_profile as B  # noqa: E402
import map_data as MD  # noqa: E402
import underwrite as U  # noqa: E402
import zip_page as ZP  # noqa: E402

_FAILS: list[str] = []
_COUNT = 0


def check(cond, msg):
    global _COUNT
    _COUNT += 1
    if not cond:
        _FAILS.append(msg)


def row(**kw):
    r = {k: None for k in B.COLUMN_NAMES}
    r.update(kw)
    return r


COMMON = dict(tax_rate_investor=1.2, tax_rate_owner=1.1, ins_landlord_300k=2000, ins_owner_300k=1500)
SF = row(zip="94105", state="CA", county="San Francisco County", city="San Francisco", lat=37.79, lng=-122.39,
         zhvi=1_126_984, zhvi_month="2026-08", zori=5958, zori_month="2026-08", zhvi_3br=3_041_046,
         hud_rent_br3=4832, acs_median_income=225_000, rdc_month="2026-09", rdc_dom=94, rdc_thin=0,
         rdc_quality_flag=1, rf_period_end="2026-05-31", rf_sale_to_list_pct=101.2, **COMMON)
DET = row(zip="48213", state="MI", county="Wayne County", city="Detroit", lat=42.4, lng=-82.99,
          zhvi=46_649, zhvi_month="2026-08", zori=1316, zori_month="2026-08", zhvi_3br=52_000,
          hud_rent_br3=1500, acs_median_income=31_000, rdc_month="2026-09", rdc_dom=45, rdc_thin=1, **COMMON)
# No Zillow rent: a HUD rent exists, but the Zillow basis must not borrow it.
TX = row(zip="78748", state="TX", county="Travis County", city="Austin", lat=30.17, lng=-97.82,
         zhvi=399_779, zhvi_month="2026-08", zhvi_3br=420_000, hud_rent_br3=2400,
         acs_median_income=None, rdc_month="2026-09", rdc_dom=60, rdc_thin=0,
         acs_flags=json.dumps({"acs_median_income": {"bound": 250001, "side": "top"}}), **COMMON)
# No city name, no Zillow at all.
RURAL = row(zip="01864", state="MA", county="Middlesex County", lat=42.58, lng=-71.08,
            acs_median_value=650_000, hud_rent_br3=2900, acs_median_income=150_000, **COMMON)

tmp = Path(tempfile.mkdtemp())
DB = tmp / "zip_profile.db"
B.write_db([TX, SF, RURAL, DET], {"built_at": "2026-10-01T07:24Z", "acs_vintage": "2020-2024 5-year",
                                  "zhvi_last_month": "2026-08", "hazards_as_of": "2026-09-28"}, DB)
RATE = 6.3

# ══════════════════════════════════════════════════════════════════
# THE REGISTRY — every metric names a real column or a real result
# ══════════════════════════════════════════════════════════════════
inv_keys = set(U.investor(300_000, 2000, tax_rate_pct=1, insurance_annual=1000, rate_pct=7))
own_keys = set(U.owner(300_000, tax_rate_pct=1, insurance_annual=1000, rate_pct=7, market_rent=2000,
                       area_income=80_000))
bad = []
for m in MD.METRICS.values():
    kind, field = m["how"].split(":", 1)
    ok = ((kind == "col" and field in B.COLUMN_NAMES) or (kind == "inv" and field in inv_keys)
          or (kind == "own" and field in own_keys))
    if not ok or m["fmt"] not in MD._ROUND or (m["thin"] and m["thin"] not in B.COLUMN_NAMES):
        bad.append(m["key"])
check(not bad, f"EVERY METRIC READS A REAL COLUMN OR UNDERWRITING RESULT, with a known format: {bad}")
check(all(MD.METRICS[k]["basis"] for k in ("cap_rate", "cash_flow", "dscr", "gross_yield"))
      and not MD.METRICS["own_payment"]["basis"] and not MD.METRICS["zhvi"]["basis"],
      "investor metrics depend on the price/rent basis; owner and published ones do not")
check(not any("score" in k or "forecast" in k or "composite" in k for k in MD.METRICS),
      "no score, forecast or composite on the map")
check(all(MD.METRICS[k]["stale"] for k in MD.METRICS if k.startswith("rf_")),
      "every Redfin metric is marked stale")

# ══════════════════════════════════════════════════════════════════
# BASE — one fixed order every metric array follows
# ══════════════════════════════════════════════════════════════════
b = MD.base(DB)
check(b["zip"] == ["01864", "48213", "78748", "94105"] and b["n"] == 4, "points are ordered by ZIP")
check(len(b["lat"]) == len(b["lng"]) == len(b["st"]) == len(b["place"]) == 4, "arrays are aligned")
check(b["states"] == ["CA", "MA", "MI", "TX"] and b["states"][b["st"][3]] == "CA", "states by index")
check(b["place"][0] == "Middlesex County" and b["place"][3] == "San Francisco",
      "a ZIP with no city name is placed by its county")
I = {z: i for i, z in enumerate(b["zip"])}

# ══════════════════════════════════════════════════════════════════
# ONE BASIS PER MAP, AND THE ZIP PAGE'S OWN ARITHMETIC
# ══════════════════════════════════════════════════════════════════
cap = MD.metric_values("cap_rate", "zillow", RATE, "Sep 24, 2026", DB)
page_cap = ZP.underwrite_zip(SF, {}, RATE)["investor"]["cap_rate_pct"]
check(cap["values"][I["94105"]] == page_cap,
      "THE MAP AND THE ZIP PAGE GIVE THE SAME CAP RATE for the same ZIP and pairing")
page_cf = ZP.underwrite_zip(SF, {}, RATE)["investor"]["cash_flow_monthly"]
check(MD.metric_values("cash_flow", "zillow", RATE, "", DB)["values"][I["94105"]] == round(page_cf),
      "and the same cash flow")
check(cap["values"][I["78748"]] is None and cap["values"][I["01864"]] is None,
      "ON THE ZILLOW BASIS A ZIP WITHOUT ZILLOW RENT HAS NO FIGURE — its HUD rent is not borrowed")
hud = MD.metric_values("cap_rate", "hud3", RATE, "", DB)
want = U.investor(420_000, 2400, tax_rate_pct=1.2, insurance_annual=MD.RA.scaled_premium(2000, 420_000),
                  rate_pct=RATE + 0.75)["cap_rate_pct"]
check(hud["values"][I["78748"]] == want and hud["basis"] == "hud3",
      "the 3-bed basis pairs Zillow's 3-bed value with HUD's 3-bed rent")
check(hud["values"][I["01864"]] is None,
      "no Zillow 3-bed value, no 3-bed figure — the Census all-homes value is not mixed in")
check(MD.metric_values("cap_rate", "nonsense", RATE, "", DB)["basis"] == MD.DEFAULT_BASIS,
      "an unknown basis falls back to the default")
gy = MD.metric_values("gross_yield", "zillow", RATE, "", DB)["values"][I["94105"]]
check(gy == round(5958 * 12 / 1_126_984 * 100, 2), "gross yield is a year's rent over price")
check(MD.metric_values("cash_flow", "zillow", None, "", DB)["values"][I["94105"]] is None
      and MD.metric_values("cap_rate", "zillow", None, "", DB)["values"][I["94105"]] == page_cap,
      "with no mortgage rate, cash flow is missing but the cap rate (no loan) still stands")

own = MD.metric_values("own_payment", "hud3", RATE, "", DB)
check(own["basis"] is None and own["values"][I["78748"]] is not None and own["values"][I["01864"]] is None,
      "owner figures use Zillow's typical home whatever the investor basis")
check(own["values"][I["94105"]] == round(ZP.underwrite_zip(SF, {}, RATE)["owner"]["monthly_total"]),
      "and match the ZIP page's owner card")

# ══════════════════════════════════════════════════════════════════
# FLAGS — kept, labelled, never silently dropped
# ══════════════════════════════════════════════════════════════════
check(cap["suspect"] == [I["48213"]] and cap["values"][I["48213"]] is not None,
      "DETROIT'S $47k HOME AT $1,316 RENT IS FLAGGED SUSPECT — and its figure is still sent")
check(MD.metric_values("own_minus_rent", "zillow", RATE, "", DB)["suspect"] == [I["48213"]],
      "the owner's own-vs-rent comparison carries the same flag")
dom = MD.metric_values("rdc_dom", "zillow", RATE, "", DB)
check(dom["thin"] == [I["48213"]] and dom["values"][I["48213"]] == 45,
      "a thin listing figure is listed as thin, with its value")
check(dom["volatile"] == [I["94105"]] and cap["volatile"] == [],
      "Realtor.com's quality flag rides with Realtor.com metrics only")
inc = MD.metric_values("acs_median_income", "zillow", RATE, "", DB)
check(inc["values"][I["78748"]] == 250001 and inc["coded"] == {I["78748"]: "top"},
      "A CENSUS TOP-CODE IS SENT AS ITS BOUND AND MARKED, so the page prints '$250,000+'")
check(MD.metric_values("nope", "zillow", RATE, "", DB) is None, "an unknown metric is None")

# ══════════════════════════════════════════════════════════════════
# DATES
# ══════════════════════════════════════════════════════════════════
check(MD.metric_values("zhvi", "zillow", RATE, "", DB)["as_of"] == "Aug 2026", "Zillow's month")
check(dom["as_of"] == "Sep 2026", "Realtor.com's month")
check(MD.metric_values("rf_sale_to_list_pct", "zillow", RATE, "", DB)["as_of"] == "May 2026",
      "Redfin's window end — May, labelled stale on the page")
check(inc["as_of"] == "2020-2024 5-year", "the Census vintage")
check(cap["as_of"] == "Aug 2026 values; rate Sep 24, 2026", "an underwritten figure names its values and rate")
check(MD.metric_values("haz_total", "zillow", RATE, "", DB)["as_of"] == "Sep 2026", "FEMA's pull")

# ══════════════════════════════════════════════════════════════════
# THE PAGE RENDERS
# ══════════════════════════════════════════════════════════════════
from jinja2 import Environment, FileSystemLoader  # noqa: E402

env = Environment(loader=FileSystemLoader(os.path.join(ROOT, "templates")))
env.globals.update(is_admin=lambda r: False, current_user=lambda r: None, pipeline_access=lambda r: False)
req = SimpleNamespace(url=SimpleNamespace(path="/map"), query_params={})
ctx = dict(request=req, registry=MD.registry(), bases=MD.BASES, default_metric=MD.DEFAULT_METRIC,
           default_basis=MD.DEFAULT_BASIS, defaults=U.DEFAULTS, rate=RATE, rate_date="Sep 24, 2026",
           meta={"built_at": "2026-10-01T07:24Z", "acs_vintage": "2020-2024 5-year"})
html = env.get_template("map.html").render(**ctx)
check(all(f'value="{k}"' in html for k in MD.METRICS), "every metric is offered in the picker")
check("25% down" in html and "7.05%" in html and "Sep 24, 2026" in html,
      "the assumptions name the down payment and the investor rate with its date")
for bad_s in (">None<", " None ", "undefined"):
    check(bad_s not in html, f"no '{bad_s}' leaks into the page")
no_rate = env.get_template("map.html").render(**{**ctx, "rate": None})
check("no mortgage rate loaded" in no_rate, "no rate: the assumptions say so")

# ── report ──
if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for x in _FAILS:
        print("  ✗", x)
    sys.exit(1)
print(f"OK — all {_COUNT} ZIP map checks passed.")
print("   one basis per map; same arithmetic as the ZIP page; thin, suspect and top-coded kept and marked")

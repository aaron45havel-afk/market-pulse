"""The ZIP page (zip_page.py + templates/zip_profile.html) on a small
fixture profile: pairings, defaults, labels, comparisons, and a full render.

Run: python tests/test_zip_page.py
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
from datetime import date
from pathlib import Path
from types import SimpleNamespace

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))

import build_zip_profile as B  # noqa: E402
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


SF = row(zip="94105", state="CA", county_fips="06075", county="San Francisco County",
         city="San Francisco", zhvi=1_126_984, zhvi_month="2026-08", zhvi_yoy_pct=9.5,
         zhvi_2br=1_522_166, zhvi_3br=3_041_046, zori=5958, zori_month="2026-08",
         hud_rent_br2=3697, hud_rent_br3=4832, hud_rent_tier="fmr", acs_rent_br2=None,
         acs_rent_br3=3200, acs_median_income=225_000, price_to_rent=15.76, price_to_income=5.0,
         tax_rate_zip=0.715, tax_rate_zip_basis="top-coded", tax_rate_investor=1.18,
         tax_basis_investor="state purchase rate (assessment resets at sale)",
         tax_rate_owner=1.10, tax_basis_owner="CA purchase floor", ins_landlord_300k=2400,
         ins_owner_300k=1500, rental_vacancy_pct=12.0,
         acs_flags=json.dumps({"acs_median_taxes": {"bound": 10001, "side": "top"},
                               "acs_rent_br1": {"bound": 3501, "side": "top"},
                               "acs_year_built": {"bound": 1939, "side": "bottom"}}),
         rdc_month="2026-09", rdc_active=55, rdc_dom=94, rdc_thin=0, rdc_quality_flag=1,
         rf_period_begin="2026-03-01", rf_period_end="2026-05-31", rf_sale_price=1_250_000, rf_thin=0,
         haz_flood=96.0, haz_wildfire=0.12, haz_wind=0.0, haz_quake=184.0, haz_total=280.12)
OAK = row(zip="94607", state="CA", county_fips="06001", county="Alameda County", zhvi=700_000,
          zhvi_yoy_pct=1.0, zori=3000, price_to_rent=19.4, rdc_dom=40, rdc_thin=0, tax_rate_zip=1.2)
SF2 = row(zip="94110", state="CA", county_fips="06075", county="San Francisco County",
          zhvi=1_400_000, zhvi_yoy_pct=4.0, rdc_dom=200, rdc_thin=1, tax_rate_zip=0.6)
RURAL = row(zip="01864", state="MA", county_fips="25017", county="Middlesex County",
            acs_median_value=650_000, acs_rent_br3=2400, hud_rent_br3=2900, hud_rent_tier="safmr",
            tax_rate_investor=1.3, tax_rate_owner=1.2, ins_landlord_300k=1800, ins_owner_300k=1600)

tmp = Path(tempfile.mkdtemp())
PROF, SER = tmp / "zip_profile.db", tmp / "zip_series.db"
B.write_db([SF, OAK, SF2, RURAL], {"built_at": "2026-10-01T07:24Z", "acs_vintage": "2020-2024 5-year"}, PROF)
B.write_series_db({"_meta": {}, "zhvi": {"94105": ["2026-06", [11000, 11100, 11270]]},
                   "zori": {"94105": ["2026-06", [5800, 5900, 5958]]}}, SER)

# ══════════════════════════════════════════════════════════════════
# PAIRINGS — the price and the rent describe the same home
# ══════════════════════════════════════════════════════════════════
opts = {o["key"]: o for o in ZP.rent_options(SF)}
check(ZP.default_option(list(opts.values()))["key"] == "zillow",
      "ZILLOW'S TYPICAL HOME WITH ZILLOW'S TYPICAL RENT is the default where both exist")
check(opts["hud_3"]["price"] == 3_041_046 and opts["hud_3"]["rent"] == 4832,
      "a 3-bed HUD rent is paired with Zillow's 3-bed VALUE, not the all-homes one")
check("county FMR" in opts["hud_3"]["rent_note"] and "40th" in opts["hud_3"]["rent_note"],
      "and says what kind of rent it is")
check(opts["acs_3"]["price"] == 3_041_046 and "existing tenants" in opts["acs_3"]["rent_note"],
      "a Census rent is labelled as existing tenants' rent, which lags the market")
check("hud_1" not in opts and "acs_2" not in opts, "no pairing is offered without both halves")
r_opts = {o["key"]: o for o in ZP.rent_options(RURAL)}
check("zillow" not in r_opts and ZP.default_option(list(r_opts.values()))["key"] == "hud_3"
      and "Small Area FMR" in r_opts["hud_3"]["rent_note"],
      "no Zillow here: the default falls to HUD 3-bed, and names the ZIP-level SAFMR")
check(r_opts["hud_3"]["price"] == 650_000 and "Census median owner value" in r_opts["hud_3"]["price_note"],
      "WITH NO ZILLOW VALUE THE PRICE IS THE CENSUS MEDIAN OWNER VALUE — and says so")

# ══════════════════════════════════════════════════════════════════
# UNDERWRITING THROUGH underwrite.py
# ══════════════════════════════════════════════════════════════════
uw = ZP.underwrite_zip(SF, {}, 7.03)
u = uw["used"]
check(u["price"] == 1_126_984 and u["rent"] == 5958 and u["pairing"] == "zillow",
      "defaults are the ZIP's own figures")
check(u["tax"] == 1.18 and u["o_tax"] == 1.10 and u["rate"] == 7.78 and u["o_rate"] == 7.03,
      "investor tax and rate (+0.75) differ from the owner's")
check(u["ins"] == 4800 and u["o_ins"] == 3000,
      "premiums scale with price and stop at 2x the $300k figure")
check(uw["investor"]["cap_rate_pct"] is not None and uw["owner"]["monthly_total"] > 0,
      "both readers' figures are computed")
ov = ZP.underwrite_zip(SF, {"price": "900000", "rent": "6000", "units": "2", "vac": "8",
                            "pairing": "hud_3", "o_down": "10"}, 7.03)
check(ov["used"]["price"] == 900_000 and ov["used"]["rent"] == 6000 and ov["used"]["units"] == 2
      and ov["used"]["vac"] == 8 and ov["owner"]["monthly"]["pmi"] > 0,
      "EVERY INPUT CAN BE OVERRIDDEN: an explicit price or rent beats the pairing's")
check(ZP.underwrite_zip(SF, {"pairing": "hud_3"}, 7.03)["used"]["price"] == 3_041_046,
      "choosing a pairing without typing a price takes the pairing's price")
check(ZP.underwrite_zip(SF, {"units": "9"}, 7.03)["used"]["units"] == 4,
      "units stop at 4 — above that is commercial lending")
check(ZP.underwrite_zip(SF, {"price": "abc"}, 7.03)["used"]["price"] == 1_126_984,
      "a junk value falls back to the default rather than breaking the card")
none_rate = ZP.underwrite_zip(SF, {}, None)
check(none_rate["used"]["rate"] is None and "rate" in none_rate["investor"]["missing"],
      "no mortgage rate: the card says so instead of assuming one")

# ══════════════════════════════════════════════════════════════════
# LABELS AND COMPARISONS
# ══════════════════════════════════════════════════════════════════
check(ZP.month_label("2026-05-31") == "May 2026" and ZP.month_label(None) == "", "month labels")
check(ZP.placeholder(SF, "acs_median_taxes") == "$10,000+"
      and ZP.placeholder(SF, "acs_rent_br1") == "$3,500+"
      and ZP.placeholder(SF, "acs_year_built") == "1939 or earlier"
      and ZP.placeholder(SF, "acs_median_income") is None,
      "CENSUS TOP-CODES READ AS '$10,000+', never as a measured $10,001")
cmp = {c["col"]: c for c in ZP.comparisons(SF, PROF)}
check(cmp["zhvi"]["county"] == ((1_126_984 + 1_400_000) / 2, 2) and cmp["zhvi"]["state"][1] == 3,
      "county and state medians across ZIPs, with how many ZIPs")
check(cmp["rdc_dom"]["county"] == (94, 1),
      "A THIN ZIP'S LISTING FIGURE STAYS OUT OF ITS COUNTY'S MEDIAN (94110's 200 days)")
check(cmp["zhvi"]["state_pct_rank"] == 67, "rank within the state: 2 of 3 CA ZIPs at or below")

page = ZP.build_page("94105", 7.03, "2026-09-24", PROF, SER, today=date(2026, 10, 1))
check(page["labels"]["rf"] == "May 2026" and page["labels"]["rf_stale"] is True
      and page["labels"]["rf_age"] == 5 and page["labels"]["rdc"] == "Sep 2026",
      "REDFIN'S MAY WINDOW IS LABELLED STALE in October; listings say September")
check(page["series"]["zhvi"] == {"start": "2026-06", "values": [1_100_000, 1_110_000, 1_127_000]}
      and page["series"]["zori"]["values"][-1] == 5958,
      "series come back in dollars with their start month")
check(ZP.build_page("99999", 7.03, "", PROF, SER) is None, "an unknown ZIP is None, not an error")
rural = ZP.build_page("01864", 7.03, "", PROF, SER)
check(rural["series"] == {} and rural["uw"]["used"]["rent"] == 2900,
      "a ZIP Zillow does not cover still gets a page and a HUD-based card")

# ══════════════════════════════════════════════════════════════════
# THE TEMPLATE RENDERS — full data, no Zillow, and not found
# ══════════════════════════════════════════════════════════════════
from jinja2 import Environment, FileSystemLoader  # noqa: E402

env = Environment(loader=FileSystemLoader(os.path.join(ROOT, "templates")))
env.globals.update(is_admin=lambda r: False, current_user=lambda r: None,
                   pipeline_access=lambda r: False)
req = SimpleNamespace(url=SimpleNamespace(path="/zip/94105"), query_params={})
tpl = env.get_template("zip_profile.html")
html = tpl.render(request=req, page=page, missing=None)
for needle in ("94105", "Rental investor", "Owner-occupant", "Market activity",
               "$10,000+", "stale", "volatile", "May 2026", "Sep 2026", "San Francisco County"):
    check(needle in html, f"the page shows '{needle}'")
for bad in ("None", "nan", "undefined", "Undefined"):
    check(f">{bad}<" not in html and f" {bad} " not in html, f"no '{bad}' leaks into the page")
check("−$" in tpl.render(request=req, page={**page, "uw": ZP.underwrite_zip(SF, {"rent": "3000"}, 7.03)},
                         missing=None), "negative cash flow prints as −$, not $-")
html2 = tpl.render(request=req, page=rural, missing=None)
check("01864" in html2 and "Zillow does not cover" in html2 and "No Zillow series" in html2,
      "the no-Zillow ZIP renders and says what it lacks")
html3 = tpl.render(request=req, page=None, missing="99999")
check("No data for ZIP 99999" in html3, "a missing ZIP renders a plain not-found page")

# ── report ──
if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for x in _FAILS:
        print("  ✗", x)
    sys.exit(1)
print(f"OK — all {_COUNT} ZIP page checks passed.")
print("   honest price/rent pairings; every input overridable; stale and thin labelled; renders")

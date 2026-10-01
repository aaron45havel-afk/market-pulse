"""Market activity for the ZIP profile (scripts/zip_market.py) and how the
build joins and carries it (scripts/build_zip_profile.py).

Run: python tests/test_zip_market.py
"""
from __future__ import annotations

import os
import sys
from datetime import date

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))

import build_zip_profile as B  # noqa: E402
import zip_market as ZM  # noqa: E402

_FAILS: list[str] = []
_COUNT = 0


def check(cond, msg):
    global _COUNT
    _COUNT += 1
    if not cond:
        _FAILS.append(msg)


# ══════════════════════════════════════════════════════════════════
# REALTOR.COM LISTINGS
# ══════════════════════════════════════════════════════════════════
RDC_HEAD = ("month_date_yyyymm,postal_code,zip_name,median_listing_price,median_listing_price_yy,"
            "active_listing_count,active_listing_count_yy,median_days_on_market,"
            "median_days_on_market_yy,new_listing_count,price_reduced_share,price_reduced_share_yy,"
            "pending_listing_count,median_listing_price_per_square_foot,median_square_feet,"
            "pending_ratio,quality_flag")
RDC = "\n".join([
    RDC_HEAD,
    # Statesville NC, September 2026, as probed
    "202609,28625,\"statesville, nc\",343750.0,-0.0833,144,0.0667,66.0,0.1441,46,0.2773,0.0566,"
    "44.0,187.0,1951.0,0.3056,0",
    "202608,28625,\"statesville, nc\",350000.0,-0.05,137,0.05,58.0,0.1,40,0.23,0.04,73.0,190.0,"
    "1950.0,0.53,0",
    "202609,2138,\"cambridge, ma\",1250000.0,0.02,4,,30.0,,2,0.25,,3.0,900.0,1400.0,0.75,1",
    "Note: quality_flag = 1 marks a month with a data-source change,,,,,,,,,,,,,,,,",
])
rdc = ZM.parse_rdc(RDC)
st = rdc["28625"]
check(st["month"] == "2026-09", "THE LATEST MONTH WINS, and says which month it is")
check(st["active"] == 144 and st["new"] == 46 and st["pending"] == 44.0 and st["dom"] == 66.0,
      "counts and days on market as published")
check(st["active_yoy_pct"] == 6.67 and st["dom_yoy_pct"] == 14.41 and st["list_price_yoy_pct"] == -8.33,
      "a '_yy' on a count or a price is a fractional change, stored as percent")
check(st["price_cut_pct"] == 27.73 and st["price_cut_yoy_pp"] == 5.66,
      "a share is stored as percent, and its '_yy' as percentage POINTS")
check(st["pending_ratio"] == 0.306 and st["list_ppsf"] == 187.0 and st["list_sqft"] == 1951.0,
      "pending ratio (pending ÷ active) and list price per square foot")
check(st["thin"] is False and st["quality_flag"] == 0, "144 listings is a market")
cam = rdc["02138"]
check(cam["active"] == 4 and cam["thin"] is True,
      "FOUR LISTINGS IS NOT A MARKET: kept, and flagged thin")
check(cam["active_yoy_pct"] is None and cam["quality_flag"] == 1,
      "a blank change stays None; Realtor.com's own quality flag is kept")
check(set(rdc) == {"28625", "02138"}, "ZIPs zero-filled; the note row skipped")
cty = ZM.parse_rdc("month_date_yyyymm,county_fips,county_name,active_listing_count\n"
                   "202609,6075,\"san francisco, ca\",900\n", key="county_fips")
check(cty["06075"]["active"] == 900, "county files key on a zero-filled FIPS code")

# ══════════════════════════════════════════════════════════════════
# REDFIN SALES (frozen)
# ══════════════════════════════════════════════════════════════════
RF_HEAD = ["PERIOD_BEGIN", "PERIOD_END", "PERIOD_DURATION", "REGION_TYPE", "REGION", "PROPERTY_TYPE",
           "MEDIAN_SALE_PRICE", "MEDIAN_SALE_PRICE_YOY", "HOMES_SOLD", "MEDIAN_PPSF",
           "AVG_SALE_TO_LIST", "SOLD_ABOVE_LIST", "MONTHS_OF_SUPPLY", "MEDIAN_DOM"]


def rf(*rows):
    return ["\t".join(RF_HEAD)] + ["\t".join(r) for r in rows]


lines = rf(
    ["2026-03-01", "2026-05-31", "90", "zip code", "Zip Code: 89178", "All Residential",
     "455000", "0.031", "215", "245.1", "0.9896", "0.214", "NA", "41"],
    ["2026-02-01", "2026-04-30", "90", "zip code", "Zip Code: 89178", "All Residential",
     "450000", "0.02", "200", "240.0", "0.98", "0.2", "3.1", "45"],
    ["2026-03-01", "2026-05-31", "90", "zip code", "Zip Code: 89178", "Single Family Residential",
     "480000", "0.03", "160", "240.0", "0.99", "0.2", "NA", "40"],
    ["2026-03-01", "2026-05-31", "90", "zip code", "Zip Code: 89178", "Multi-Family (2-4 Unit)",
     "620000", "NA", "6", "200.0", "0.97", "0.1", "NA", "60"],
    ["2026-03-01", "2026-05-31", "90", "zip code", "Zip Code: 89178", "Condo/Co-op",
     "300000", "0.01", "40", "300.0", "0.98", "0.1", "NA", "50"],
    ["2026-03-01", "2026-05-31", "90", "zip code", "Zip Code: 1001", "All Residential",
     "310000", "0.05", "8", "200.0", "1.01", "0.4", "NA", "20"],
)
red = ZM.parse_redfin(lines)
v = red["89178"]
check(v["period_end"] == "2026-05-31" and v["period_begin"] == "2026-03-01",
      "THE LATEST 90-DAY WINDOW, with both ends of it")
check(v["sale_price"] == 455000 and v["homes_sold"] == 215 and v["sold_dom"] == 41
      and v["sale_ppsf"] == 245.1,
      "All Residential supplies the sale figures")
check(v["sale_to_list_pct"] == 98.96 and v["sold_above_list_pct"] == 21.4
      and v["sale_price_yoy_pct"] == 3.1,
      "ratios stored as percent")
check(v["months_supply"] is None, "'NA' is None, not zero")
check(v["sfr_sale_price"] == 480000 and v["mf24_sale_price"] == 620000 and v["mf24_homes_sold"] == 6,
      "single-family and 2-4 unit sale prices kept beside the all-homes figure")
check("condo_sale_price" not in v, "property types not asked for are ignored")
check(v["thin"] is False and red["01001"]["thin"] is True,
      "eight sales in ninety days is flagged thin; ZIPs zero-filled from 'Zip Code: 1001'")
gone = ZM.parse_redfin(rf(
    ["2026-03-01", "2026-05-31", "90", "zip code", "Zip Code: 89178", "All Residential",
     "455000", "0.031", "215", "245.1", "0.9896", "0.214", "NA", "41"],
    ["2012-02-01", "2012-04-30", "90", "zip code", "Zip Code: 59001", "All Residential",
     "90000", "0.1", "12", "80.0", "0.95", "0.1", "NA", "120"]))
check("59001" not in gone and "89178" in gone,
      "A ZIP REDFIN STOPPED REPORTING IN 2012 HAS NO 'LATEST' FIGURES: only windows "
      "ending on the file's final period count")
try:
    ZM.parse_redfin(["A\tB", "1\t2"])
    check(False, "a changed Redfin header must fail loudly")
except ValueError:
    check(True, "")

check(ZM.months_old("2026-05-31", date(2026, 10, 1)) == 5
      and ZM.months_old("2026-09", date(2026, 10, 1)) == 1 and ZM.months_old(None, date.today()) is None,
      "how old a period is, in months — what a page reads to label it stale")
check(ZM.yyyymm("202609") == "2026-09" and ZM.yyyymm("2026") is None, "month parsing")

# ══════════════════════════════════════════════════════════════════
# THE JOIN AND THE CARRY
# ══════════════════════════════════════════════════════════════════
geo = {"28625": {"lat": 35.8, "lng": -80.9, "aland_km2": 300.0},
       "89178": {"lat": 36.0, "lng": -115.3, "aland_km2": 50.0}}
county = {"28625": {"state": "NC", "county_fips": "37097", "county": "Iredell County"},
          "89178": {"state": "NV", "county_fips": "32003", "county": "Clark County"}}
cty_rdc = {"37097": {"month": "2026-09", "active": 900, "active_yoy_pct": 12.0, "pending_ratio": 0.4,
                     "dom": 60.0, "price_cut_pct": 25.0, "list_price": 389000, "list_price_yoy_pct": -2.0}}
rows = B.build_rows(geo, county, {}, {}, {}, {}, {}, {}, {},
                    {"rdc_zip": rdc, "rdc_county": cty_rdc, "redfin": red})
by = {r["zip"]: r for r in rows}
check(by["28625"]["rdc_month"] == "2026-09" and by["28625"]["rdc_active"] == 144
      and by["28625"]["rdc_thin"] == 0,
      "listings land under rdc_, with their month")
check(by["28625"]["cty_rdc_dom"] == 60.0 and by["28625"]["cty_rdc_month"] == "2026-09"
      and by["89178"]["cty_rdc_dom"] is None,
      "THE COUNTY'S FIGURES UNDER cty_rdc_ — never mistaken for the ZIP's")
check(by["89178"]["rf_period_end"] == "2026-05-31" and by["89178"]["rf_sale_to_list_pct"] == 98.96
      and by["89178"]["rf_thin"] == 0 and by["28625"]["rf_sale_price"] is None,
      "sales land under rf_ with their window's end date")
check(all(c in B.COLUMN_NAMES for c in by["89178"]), "every market key is a schema column")

prior = {"28625": {"zip": "28625", "rdc_month": "2026-08", "rdc_active": 137, "zhvi": 999,
                   "rf_period_end": "2026-05-31"}}
fresh = B.build_rows(geo, county, {}, {}, {}, {}, {}, {}, {}, {"rdc_county": cty_rdc})
n = B.carry_forward(fresh, prior, ("rdc_",))
f = {r["zip"]: r for r in fresh}
check(n == 1 and f["28625"]["rdc_month"] == "2026-08" and f["28625"]["rdc_active"] == 137,
      "A FAILED FETCH CARRIES LAST MONTH'S FIGURES WITH LAST MONTH'S DATE")
check(f["28625"]["zhvi"] is None and f["28625"]["rf_period_end"] is None,
      "and only the failed source's columns — nothing else is overwritten")

# ── report ──
if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for x in _FAILS:
        print("  ✗", x)
    sys.exit(1)
print(f"OK — all {_COUNT} ZIP market checks passed.")
print("   listings (Realtor.com) current and dated; sales (Redfin) frozen and dated; thin flagged")

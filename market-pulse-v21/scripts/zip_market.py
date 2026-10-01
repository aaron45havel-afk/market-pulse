"""Market activity for the ZIP profile: listings now, and sales as of when.

TWO SOURCES, BECAUSE NEITHER IS COMPLETE (probed 2026-10-01):

  * Realtor.com Economic Research "Inventory Core Metrics" — monthly, by ZIP
    (28.9k), county and metro, updated at the end of each month. LISTINGS
    only: active, new and pending listings, median days on market, the
    share of listings with a price cut, list price and list $/sqft. Current
    (September 2026 on Sept 30). Free with attribution to Realtor.com.
  * Redfin Data Center market tracker — the only free ZIP-level SALES data:
    median sale price, homes sold, sale-to-list, sold above list, months of
    supply. Rolling 90-day windows by property type. FROZEN: every monthly
    tracker (ZIP, county, metro, state, national) ends 2026-05-31 and was
    last written 2026-06-02; the weekly file ends 2026-04-26. Kept because
    nothing else measures sales by ZIP, and stored with its period end so
    no page can present it as current.

Every value carries its month. A figure built from fewer than THIN_COUNT
listings or sales is flagged thin rather than hidden.

Parsers are pure; fetching is a thin layer the build calls in Actions.
"""
from __future__ import annotations

import csv
import gzip
import io
import logging
import time
import urllib.error
import urllib.request
from datetime import date

log = logging.getLogger("zip_market")

RDC = "https://econdata.s3-us-west-2.amazonaws.com/Reports/Core/"
RDC_ZIP_URL = RDC + "RDC_Inventory_Core_Metrics_Zip.csv"
RDC_COUNTY_URL = RDC + "RDC_Inventory_Core_Metrics_County.csv"
# Monthly since 2016-07; the state file keys on a two-letter state_id (all 50
# + DC), the country file on country = "United States".
RDC_STATE_HISTORY_URL = RDC + "RDC_Inventory_Core_Metrics_State_History.csv"
RDC_COUNTRY_HISTORY_URL = RDC + "RDC_Inventory_Core_Metrics_Country_History.csv"
REDFIN_ZIP_URL = ("https://redfin-public-data.s3.us-west-2.amazonaws.com/"
                  "redfin_market_tracker/zip_code_market_tracker.tsv000.gz")
UA = {"User-Agent": "MarketPulse/1.0 (zip market; invoice@archfms.com)"}

# Below this many listings (Realtor.com) or sales in the 90-day window
# (Redfin) a median is a handful of homes, not a market.
THIN_COUNT = 10

# Realtor.com: (source column, our column, scale). "_yy" on a count or a
# price is a fractional change (0.14 = +14%); on a share it is the change
# in the share (0.057 = +5.7 points). Both are stored ×100.
RDC_FIELDS = (
    ("active_listing_count", "active", 1),
    ("active_listing_count_yy", "active_yoy_pct", 100),
    ("new_listing_count", "new", 1),
    ("pending_listing_count", "pending", 1),
    ("pending_ratio", "pending_ratio", 1),
    ("median_days_on_market", "dom", 1),
    ("median_days_on_market_yy", "dom_yoy_pct", 100),
    ("price_reduced_share", "price_cut_pct", 100),
    ("price_reduced_share_yy", "price_cut_yoy_pp", 100),
    ("median_listing_price", "list_price", 1),
    ("median_listing_price_yy", "list_price_yoy_pct", 100),
    ("median_listing_price_per_square_foot", "list_ppsf", 1),
    ("median_square_feet", "list_sqft", 1),
    ("quality_flag", "quality_flag", 1),
)

# State and national history kept for /conditions: this month, the same month
# a year earlier, and every month between — so a year-over-year change compares
# the same season, and a chart can draw this year against last.
STATE_MONTHS = 25
STATE_FIELDS = (
    ("active_listing_count", "active", 1),
    ("new_listing_count", "new", 1),
    ("pending_listing_count", "pending", 1),
    ("pending_ratio", "pending_ratio", 1),
    ("median_days_on_market", "dom", 1),
    ("price_reduced_share", "price_cut_pct", 100),
    ("median_listing_price", "list_price", 1),
    ("median_listing_price_per_square_foot", "list_ppsf", 1),
    ("quality_flag", "quality_flag", 1),
)
STATE_COLUMNS = ("geo", "month") + tuple(dst for _, dst, _ in STATE_FIELDS)
# Decimals kept where three would blur a year-over-year change: the ratio
# moves in the third decimal (0.3641 → 0.3295 is −0.0346).
STATE_DIGITS = {"pending_ratio": 4}

# Redfin: the property types kept, and the fields read from each.
REDFIN_TYPES = {"All Residential": "all", "Single Family Residential": "sfr",
                "Multi-Family (2-4 Unit)": "mf24"}
REDFIN_FIELDS = (
    ("MEDIAN_SALE_PRICE", "sale_price", 1),
    ("MEDIAN_SALE_PRICE_YOY", "sale_price_yoy_pct", 100),
    ("HOMES_SOLD", "homes_sold", 1),
    ("MEDIAN_PPSF", "sale_ppsf", 1),
    ("AVG_SALE_TO_LIST", "sale_to_list_pct", 100),
    ("SOLD_ABOVE_LIST", "sold_above_list_pct", 100),
    ("MONTHS_OF_SUPPLY", "months_supply", 1),
    ("MEDIAN_DOM", "sold_dom", 1),
)
# Kept per property type; everything else only for All Residential.
REDFIN_PER_TYPE = ("sale_price", "homes_sold")


def _num(v):
    v = (v or "").strip()
    if not v or v.upper() in ("NA", "NAN", "NULL"):
        return None
    try:
        f = float(v)
    except ValueError:
        return None
    return None if f != f else f


def _scaled(v, scale, digits=None):
    n = _num(v)
    if digits is None:
        digits = 2 if scale != 1 else 3
    return None if n is None else round(n * scale, digits)


def yyyymm(v: str) -> str | None:
    """'202609' → '2026-09'."""
    v = (v or "").strip()
    return f"{v[:4]}-{v[4:6]}" if len(v) == 6 and v.isdigit() else None


def parse_rdc(text: str, key: str = "postal_code") -> dict:
    """A Realtor.com core-metrics CSV → {key: {month, <fields>, thin}} for
    its LATEST month. `key` is 'postal_code' (ZIP) or 'county_fips'.
    Rows whose key is not all digits (notes, totals) are skipped."""
    rd = csv.DictReader(io.StringIO(text))
    width = 5
    out: dict = {}
    for row in rd:
        k = (row.get(key) or "").strip()
        if not k.isdigit():
            continue
        k = k.zfill(width)
        month = yyyymm(row.get("month_date_yyyymm"))
        if not month:
            continue
        if k in out and out[k]["month"] >= month:
            continue
        rec = {"month": month}
        for src, dst, scale in RDC_FIELDS:
            rec[dst] = _scaled(row.get(src), scale)
        if rec["quality_flag"] is not None:
            rec["quality_flag"] = int(rec["quality_flag"])
        active = rec["active"]
        rec["thin"] = active is None or active < THIN_COUNT
        out[k] = rec
    return out


def parse_rdc_history(text: str, key: str = "state_id", months: int = STATE_MONTHS) -> dict:
    """A Realtor.com core-metrics HISTORY CSV → {geo: [record, ...]} with each
    geo's last `months` months, oldest first. `key` is 'state_id' (two
    letters, kept upper-case) or 'country' ("United States" becomes "US").
    Rows with any other key (notes, totals, a 'US' row in the state file,
    which would collide with the national one) are skipped."""
    rd = csv.DictReader(io.StringIO(text))
    out: dict = {}
    for row in rd:
        k = (row.get(key) or "").strip()
        if key == "country":
            k = "US" if k == "United States" else ""
        else:
            k = k.upper()
            if not (len(k) == 2 and k.isalpha()) or k == "US":
                continue
        month = yyyymm(row.get("month_date_yyyymm"))
        if not k or not month:
            continue
        rec = {"month": month}
        for src, dst, scale in STATE_FIELDS:
            rec[dst] = _scaled(row.get(src), scale, STATE_DIGITS.get(dst))
        if rec["quality_flag"] is not None:
            rec["quality_flag"] = int(rec["quality_flag"])
        out.setdefault(k, {})[month] = rec
    return {k: [v[m] for m in sorted(v)[-months:]] for k, v in out.items()}


def parse_redfin(lines) -> dict:
    """Redfin ZIP tracker lines (tab-separated, header first) → {zip: {...}}
    from each ZIP's LATEST 90-day window. All Residential supplies every
    field; single-family and 2-4 unit supply sale price and homes sold."""
    rd = csv.reader(lines, delimiter="\t")
    header = [h.strip().upper() for h in next(rd)]
    ix = {h: i for i, h in enumerate(header)}
    need = ("PERIOD_BEGIN", "PERIOD_END", "REGION", "PROPERTY_TYPE")
    missing = [c for c in need if c not in ix]
    if missing:
        raise ValueError(f"Redfin tracker header lacks {missing}")
    best: dict = {}
    for row in rd:
        if len(row) < len(header):
            continue
        ptype = REDFIN_TYPES.get(row[ix["PROPERTY_TYPE"]].strip())
        if not ptype:
            continue
        region = row[ix["REGION"]].strip()
        z = region.rsplit(":", 1)[-1].strip().zfill(5)
        if not (len(z) == 5 and z.isdigit()):
            continue
        end = row[ix["PERIOD_END"]].strip()
        cur = best.get((z, ptype))
        if cur and cur[0] >= end:
            continue
        best[(z, ptype)] = (end, row[ix["PERIOD_BEGIN"]].strip(), row)
    # ONLY THE FILE'S FINAL WINDOW. A ZIP whose newest row is older is one
    # Redfin stopped reporting — its "latest" figures went back to 2012 for
    # 3,302 ZIPs on the 2026-06 file. Every reporting ZIP shares the final
    # period, so anything short of it is a ZIP the tracker no longer covers.
    final = {}
    for (_z, ptype), (end, _b, _r) in best.items():
        final[ptype] = max(final.get(ptype, ""), end)
    out: dict = {}
    for (z, ptype), (end, begin, row) in best.items():
        if end < final[ptype]:
            continue
        rec = out.setdefault(z, {})
        vals = {dst: _scaled(row[ix[src]], scale)
                for src, dst, scale in REDFIN_FIELDS if src in ix}
        if ptype == "all":
            rec.update(vals)
            rec["period_end"], rec["period_begin"] = end, begin
            sold = vals.get("homes_sold")
            rec["thin"] = sold is None or sold < THIN_COUNT
        else:
            for f in REDFIN_PER_TYPE:
                rec[f"{ptype}_{f}"] = vals.get(f)
            rec[f"{ptype}_period_end"] = end
    return out


def months_old(period: str | None, today: date) -> int | None:
    """Whole months between a 'YYYY-MM' or 'YYYY-MM-DD' period and today."""
    if not period or len(period) < 7:
        return None
    try:
        y, m = int(period[:4]), int(period[5:7])
    except ValueError:
        return None
    return (today.year - y) * 12 + (today.month - m)


# ── fetching ─────────────────────────────────────────────────────────

def _open(url: str, timeout: int = 300):
    return urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=timeout)


def _retry(fn, label: str, attempts: int = 3):
    last = None
    for i in range(attempts):
        try:
            return fn()
        except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as e:
            last = e
            log.warning("  %s failed (%s), retry %d", label, e, i + 1)
            time.sleep(5 * (i + 1))
    raise RuntimeError(f"{label}: {last}")


def last_modified(url: str) -> str:
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers=UA, method="HEAD"),
                                    timeout=60) as r:
            return r.headers.get("Last-Modified", "")
    except (urllib.error.URLError, TimeoutError, OSError):
        return ""


def fetch_rdc(url: str, key: str) -> dict:
    def go():
        with _open(url) as r:
            return parse_rdc(r.read().decode("utf-8", "replace"), key)
    return _retry(go, url)


def fetch_rdc_history(url: str, key: str) -> dict:
    def go():
        with _open(url) as r:
            return parse_rdc_history(r.read().decode("utf-8", "replace"), key)
    return _retry(go, url)


def fetch_redfin(url: str = REDFIN_ZIP_URL) -> dict:
    """Streams the 1.5 GB gzip; ~80-90 s on a GitHub runner."""
    def go():
        with _open(url, timeout=900) as r:
            text = io.TextIOWrapper(gzip.GzipFile(fileobj=r), encoding="utf-8",
                                    errors="replace", newline="")
            return parse_redfin(text)
    return _retry(go, url, attempts=2)

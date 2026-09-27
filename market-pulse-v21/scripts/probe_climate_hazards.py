"""TEMPORARY probe, round 3 — FEMA loss fields end to end, and keyless Census.

Round 2 found FEMA's official ArcGIS layer reachable and showed that its
headline hazard RATINGS fold in social vulnerability. This round pulls every
tract's expected-annual-building-loss and building-value fields — a dry run
of the real fetch — and reports how nulls, "Not Applicable" and "Insufficient
Data" are encoded. It also tests whether the Census ACS API answers without a
key. Deleted before anything merges.
"""
import collections
import json
import statistics
import sys
import time
import urllib.parse
import urllib.request

UA = {"User-Agent": "MarketPulse/1.0 (probe; invoice@archfms.com)"}
FS = ("https://services.arcgis.com/XG15cJAlne2vxtgt/arcgis/rest/services/"
      "National_Risk_Index_Census_Tracts/FeatureServer/0/query")
HAZ = ("IFLD", "CFLD", "WFIR", "HRCN", "TRND", "HWAV", "WNTW", "CWAV")


def get(url, timeout=180):
    req = urllib.request.Request(url, headers=UA)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read()
    except Exception as e:                                   # noqa: BLE001
        body = b""
        try:
            body = e.read()[:400]                            # type: ignore[attr-defined]
        except Exception:                                    # noqa: BLE001
            pass
        return getattr(e, "code", None) or repr(e)[:200], body


def section(t):
    print("\n" + "=" * 78 + f"\n{t}\n" + "=" * 78, flush=True)


# ── Census ACS without a key ────────────────────────────────────────
section("Census ACS — keyless requests")
for year in (2022, 2023, 2024):
    url = (f"https://api.census.gov/data/{year}/acs/acs5?get=NAME,B19013_001E,B25003_003E"
           f"&for=zip%20code%20tabulation%20area:*")
    t0 = time.time()
    st, body = get(url, timeout=300)
    head = body[:160].decode("utf-8", "replace").replace("\n", " ")
    rows = None
    try:
        rows = len(json.loads(body)) - 1
    except Exception:                                        # noqa: BLE001
        pass
    print(f"  {year}: status {st} | {len(body):,} bytes | {time.time()-t0:.1f}s | rows {rows} | {head!r}")

# ── FEMA NRI: every tract, the loss fields ──────────────────────────
section("FEMA NRI — all tracts, loss fields")
fields = ["TRACTFIPS", "STATEABBRV", "BUILDVALUE", "NRI_VER"]
for h in HAZ:
    fields += [f"{h}_EALB", f"{h}_EXPB", f"{h}_EALR", f"{h}_RISKR"]
rows, offset, t0 = [], 0, time.time()
while True:
    qs = urllib.parse.urlencode({"where": "1=1", "outFields": ",".join(fields),
                                 "returnGeometry": "false", "orderByFields": "OBJECTID",
                                 "resultOffset": offset, "resultRecordCount": 2000, "f": "json"})
    st, body = get(FS + "?" + qs)
    try:
        page = json.loads(body)
    except Exception:                                        # noqa: BLE001
        print("  unparsable page at offset", offset, st, body[:200])
        break
    if "error" in page:
        print("  ERROR at offset", offset, page["error"])
        break
    feats = page.get("features", [])
    rows += [f["attributes"] for f in feats]
    if not page.get("exceededTransferLimit") and len(feats) < 2000:
        break
    offset += len(feats)
print(f"  {len(rows):,} tracts in {time.time()-t0:.0f}s | versions {collections.Counter(r.get('NRI_VER') for r in rows)}")
dupes = len(rows) - len({r['TRACTFIPS'] for r in rows})
print(f"  duplicate TRACTFIPS: {dupes}")
bv = [r["BUILDVALUE"] for r in rows]
print(f"  BUILDVALUE: null {sum(v is None for v in bv):,} | zero {sum(v == 0 for v in bv if v is not None):,} | "
      f"median ${statistics.median([v for v in bv if v]):,.0f}")
for h in HAZ:
    by = collections.defaultdict(lambda: collections.Counter())
    rates = []
    for r in rows:
        ealr = r.get(f"{h}_EALR")
        ealb, expb = r.get(f"{h}_EALB"), r.get(f"{h}_EXPB")
        by[ealr]["n"] += 1
        by[ealr]["ealb_null"] += ealb is None
        by[ealr]["ealb_zero"] += ealb == 0
        by[ealr]["expb_null"] += expb is None
        if ealb is not None and r.get("BUILDVALUE"):
            rates.append(ealb / r["BUILDVALUE"])
    print(f"\n  {h}:")
    for k, c in sorted(by.items(), key=lambda kv: -kv[1]["n"]):
        print(f"    EALR={k!r:24} n={c['n']:6,} EALB null={c['ealb_null']:6,} zero={c['ealb_zero']:6,} EXPB null={c['expb_null']:6,}")
    if rates:
        rates.sort()
        q = lambda p: rates[int(p * (len(rates) - 1))]      # noqa: E731
        print(f"    EALB/BUILDVALUE per $100k/yr: p50 ${q(.5)*1e5:,.2f}  p75 ${q(.75)*1e5:,.2f}  "
              f"p90 ${q(.9)*1e5:,.2f}  p99 ${q(.99)*1e5:,.2f}  max ${rates[-1]*1e5:,.2f}")
    agree = collections.Counter((r.get(f"{h}_EALR"), r.get(f"{h}_RISKR")) for r in rows)
    print("    EALR vs RISKR disagreements (top):",
          [(k, v) for k, v in agree.most_common(40) if k[0] != k[1]][:6])

# Sample Ohio flood rows so the numbers can be sanity-read.
oh = [r for r in rows if r.get("STATEABBRV") == "OH" and r.get("IFLD_EALB")]
oh.sort(key=lambda r: -(r["IFLD_EALB"] / r["BUILDVALUE"]) if r.get("BUILDVALUE") else 0)
print("\n  Ohio's 3 worst inland-flood tracts by loss rate:")
for r in oh[:3]:
    print(f"    {r['TRACTFIPS']} bldg ${r['BUILDVALUE']:,.0f} IFLD EAL ${r['IFLD_EALB']:,.0f}/yr "
          f"= ${r['IFLD_EALB']/r['BUILDVALUE']*1e5:,.0f} per $100k | EALR {r['IFLD_EALR']} RISKR {r['IFLD_RISKR']}")

print("\nPROBE DONE", flush=True)
sys.exit(0)

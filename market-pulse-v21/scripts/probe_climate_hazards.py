"""TEMPORARY probe, round 2 — edge cases in NOAA, and a route to FEMA's data.

Round 1 established the NOAA and Census formats and that hazards.fema.gov
and www.fema.gov answer 403 to GitHub's runners. This round (a) reads the
whole NOAA annual/seasonal archive to count how missing and provisional
values are actually encoded, and (b) tries FEMA's official ArcGIS feature
service and a browser-like request. Deleted before anything merges.
"""
import collections
import io
import csv
import json
import re
import sys
import tarfile
import time
import urllib.parse
import urllib.request

UA = {"User-Agent": "MarketPulse/1.0 (probe; invoice@archfms.com)"}
BROWSER = {"User-Agent": ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"),
           "Accept": "text/html,application/xhtml+xml,application/zip,*/*;q=0.8",
           "Accept-Language": "en-US,en;q=0.9"}


def get(url, headers=UA, limit=None, timeout=180):
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, dict(r.headers), (r.read(limit) if limit else r.read())
    except Exception as e:                                   # noqa: BLE001
        body = b""
        try:
            body = e.read()[:300]                            # type: ignore[attr-defined]
        except Exception:                                    # noqa: BLE001
            pass
        return getattr(e, "code", None) or repr(e)[:200], {}, body


def section(t):
    print("\n" + "=" * 78 + f"\n{t}\n" + "=" * 78, flush=True)


# ── (a) NOAA: the whole annual/seasonal archive ─────────────────────
section("NOAA annual/seasonal archive — encoding census")
url = ("https://www.ncei.noaa.gov/data/normals-annualseasonal/1991-2020/archive/"
       "us-climate-normals_1991-2020_v1.0.1_annualseasonal_multivariate_by-station_c20230404.tar.gz")
t0 = time.time()
st, hd, body = get(url, timeout=900)
print(f"status {st} | {len(body):,} bytes | {time.time() - t0:.0f}s")
FIELDS = ("DJF-TMIN-NORMAL", "JJA-TMAX-NORMAL", "ANN-TMAX-AVGNDS-GRTH090",
          "ANN-TMIN-AVGNDS-LSTH032", "ANN-SNOW-NORMAL", "ANN-PRCP-NORMAL")
present = collections.Counter()
nonnum = collections.Counter()
comp = collections.defaultdict(collections.Counter)
meas = collections.defaultdict(collections.Counter)
extreme = collections.defaultdict(list)
n_files = n_rows = 0
countries = collections.Counter()
if body:
    tf = tarfile.open(fileobj=io.BytesIO(body), mode="r:gz")
    names = [m for m in tf.getmembers() if m.isfile() and m.name.endswith(".csv")]
    print("members:", len(names), "| sample:", [m.name for m in names[:3]])
    for m in names:
        n_files += 1
        rows = list(csv.DictReader(io.TextIOWrapper(tf.extractfile(m), encoding="utf-8", errors="replace")))
        for r in rows:
            n_rows += 1
            countries[(r.get("STATION") or "??")[:2]] += 1
            for f in FIELDS:
                if f not in r:
                    continue
                v = (r.get(f) or "").strip()
                present[f] += 1
                try:
                    x = float(v)
                    if x <= -555 or x >= 9000:
                        extreme[f].append((r["STATION"], v))
                except ValueError:
                    nonnum[(f, v)] += 1
                comp[f][(r.get("comp_flag_" + f) or "").strip() or "<blank>"] += 1
                meas[f][(r.get("meas_flag_" + f) or "").strip() or "<blank>"] += 1
    print(f"files {n_files:,} | rows {n_rows:,} | rows per file max 1? {n_rows == n_files}")
    print("station prefixes (first 2 chars) top:", countries.most_common(12))
    for f in FIELDS:
        print(f"\n  {f}: present in {present[f]:,} stations")
        print("    comp flags:", dict(comp[f]))
        print("    meas flags:", dict(meas[f]))
        print("    non-numeric values:", {k[1]: c for k, c in nonnum.items() if k[0] == f})
        print("    sentinel-looking values (<=-555 or >=9000):", len(extreme[f]), extreme[f][:6])

# ── (b) FEMA NRI: other routes ──────────────────────────────────────
section("FEMA NRI — browser-like request to the static download")
for u in ("https://hazards.fema.gov/nri/Content/StaticDocuments/DataDownload//NRI_Table_CensusTracts/NRI_Table_CensusTracts.zip",
          "https://hazards.fema.gov/nri/data-resources"):
    st, hd, body = get(u, headers=BROWSER, limit=2_000_000)
    print(f"{u}\n  status {st} | bytes {len(body):,} | first bytes {body[:60]!r}")

section("ArcGIS Online — find FEMA's NRI tract item")
q = urllib.parse.urlencode({"q": 'title:"National Risk Index" AND owner:FEMA_NRI OR title:"National Risk Index Census Tracts"',
                            "f": "json", "num": 20})
st, hd, body = get("https://www.arcgis.com/sharing/rest/search?" + q)
print("search status", st)
items = []
try:
    items = json.loads(body).get("results", [])
except Exception as e:                                       # noqa: BLE001
    print("  unparsable:", body[:200])
for it in items:
    print(f"  {it.get('id')} | {it.get('type'):24} | owner {it.get('owner'):18} | "
          f"modified {time.strftime('%Y-%m-%d', time.gmtime((it.get('modified') or 0) / 1000))} | "
          f"{it.get('title')} | {it.get('url')}")

section("ArcGIS FeatureServer — NRI census tracts")
fs = "https://services.arcgis.com/XG15cJAlne2vxtgt/arcgis/rest/services/National_Risk_Index_Census_Tracts/FeatureServer/0"
st, hd, body = get(fs + "?f=json")
print("layer status", st)
try:
    meta = json.loads(body)
    fields = [f["name"] for f in meta.get("fields", [])]
    print(f"  name {meta.get('name')!r} | maxRecordCount {meta.get('maxRecordCount')} | "
          f"{len(fields)} fields | editingInfo {meta.get('editingInfo')}")
    print("  description:", (meta.get("description") or "")[:400].replace("\n", " "))
    keep = [f for f in fields if re.match(
        r"(OBJECTID|TRACTFIPS|STCOFIPS|STATEABBRV|COUNTY|NRI_VER|RISK_SCORE|RISK_RATNG|"
        r"(CFLD|RFLD|IFLD|WFIR|HRCN|TRND|HWAV|CWAV|WNTW|ERQK)_(RISKS|RISKR))$", f)]
    print("  fields of interest:", keep)
    print("  ALL fields:", fields)
    qs = urllib.parse.urlencode({"where": "1=1", "returnCountOnly": "true", "f": "json"})
    st, hd, body = get(fs + "/query?" + qs)
    print("  count:", st, body[:200])
    qs = urllib.parse.urlencode({"where": "STATEABBRV='OH'", "outFields": ",".join(keep),
                                 "returnGeometry": "false", "resultRecordCount": 3, "f": "json"})
    st, hd, body = get(fs + "/query?" + qs)
    print("  sample status", st)
    for feat in json.loads(body).get("features", [])[:3]:
        print("   ", feat.get("attributes"))
    # How "not applicable" and "insufficient data" are encoded, for flood and wildfire
    for fld in [f for f in keep if f.endswith("_RISKR")][:6]:
        qs = urllib.parse.urlencode({"where": "1=1", "groupByFieldsForStatistics": fld,
                                     "outStatistics": json.dumps([{"statisticType": "count",
                                                                   "onStatisticField": "OBJECTID",
                                                                   "outStatisticFieldName": "n"}]),
                                     "f": "json"})
        st, hd, body = get(fs + "/query?" + qs)
        try:
            groups = {f["attributes"][fld]: f["attributes"]["n"] for f in json.loads(body).get("features", [])}
        except Exception:                                    # noqa: BLE001
            groups = body[:200]
        print(f"  {fld} values:", groups)
    for fld in [f for f in keep if f.endswith("_RISKS")][:6]:
        qs = urllib.parse.urlencode({"where": f"{fld} IS NULL", "returnCountOnly": "true", "f": "json"})
        st, hd, body = get(fs + "/query?" + qs)
        print(f"  {fld} IS NULL count:", body[:120])
except Exception as e:                                       # noqa: BLE001
    print("  unparsable layer:", repr(e)[:200], body[:300])

print("\nPROBE DONE", flush=True)
sys.exit(0)

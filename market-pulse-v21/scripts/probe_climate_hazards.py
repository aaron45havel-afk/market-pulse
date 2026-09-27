"""TEMPORARY probe — print the real shape of the weather and hazard sources.

The dev sandbox cannot reach NOAA, FEMA or the Census, and a parser written
against remembered column names is how a job ends up green while reading
nothing (see the CENSUS_API_KEY entry in BACKLOG.md). This runs once in
Actions, prints listings, headers and sample rows, and is deleted before
anything merges.
"""
import csv
import io
import re
import sys
import urllib.request
import zipfile

UA = {"User-Agent": "MarketPulse/1.0 (probe; invoice@archfms.com)"}


def get(url, limit=None, timeout=120):
    req = urllib.request.Request(url, headers=UA)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read(limit) if limit else r.read()
            return r.status, dict(r.headers), body
    except Exception as e:                                   # noqa: BLE001
        return getattr(e, "code", None) or repr(e)[:200], {}, b""


def hrefs(body):
    return re.findall(r'href="([^"]+)"', body.decode("utf-8", "replace"))


def section(t):
    print("\n" + "=" * 78 + f"\n{t}\n" + "=" * 78, flush=True)


# ── NOAA annual/seasonal + monthly normals ──────────────────────────
for kind in ("normals-annualseasonal", "normals-monthly"):
    base = f"https://www.ncei.noaa.gov/data/{kind}/1991-2020/"
    section(f"NOAA {kind}: {base}")
    st, hd, body = get(base)
    print("status", st, "| links:", [h for h in hrefs(body) if not h.startswith("?")][:30])
    for sub in ("archive/", "doc/"):
        st, hd, body = get(base + sub)
        links = [h for h in hrefs(body) if not h.startswith("?")]
        print(f"  {sub} status {st} | {len(links)} links:", links[:25])
    st, hd, body = get(base + "access/")
    csvs = [h for h in hrefs(body) if h.endswith(".csv")]
    print(f"  access/ status {st} | {len(csvs)} station CSVs; first:", csvs[:5])
    for sid in ("USW00014820.csv", "USW00014821.csv", csvs[0] if csvs else None):
        if not sid:
            continue
        st, hd, body = get(base + "access/" + sid)
        print(f"\n  --- {sid}: status {st}, {len(body)} bytes")
        if body:
            rows = list(csv.reader(io.StringIO(body.decode("utf-8", "replace"))))
            print("  HEADER (%d cols):" % len(rows[0]), rows[0])
            for r in rows[1:3]:
                print("  ROW:", dict(zip(rows[0], r)))
            print(f"  total data rows: {len(rows) - 1}")
        break_after = kind == "normals-monthly"
        if break_after:
            break

# ── FEMA National Risk Index ────────────────────────────────────────
section("FEMA NRI — locate downloads")
for page in ("https://hazards.fema.gov/nri/data-resources",
             "https://www.fema.gov/about/openfema/data-sets/national-risk-index-data"):
    st, hd, body = get(page)
    links = [h for h in hrefs(body) if re.search(r"\.(zip|csv|xlsx|pdf)(\?|$)", h, re.I)
             or "Table" in h or "DataDownload" in h]
    print(f"{page}\n  status {st} | candidate links ({len(links)}):")
    for h in links[:40]:
        print("   ", h)

candidates = [
    "https://hazards.fema.gov/nri/Content/StaticDocuments/DataDownload//NRI_Table_CensusTracts/NRI_Table_CensusTracts.zip",
    "https://hazards.fema.gov/nri/Content/StaticDocuments/DataDownload/NRI_Table_CensusTracts/NRI_Table_CensusTracts.zip",
]
for url in candidates:
    section(f"FEMA NRI tract table: {url}")
    st, hd, body = get(url, timeout=600)
    print("status", st, "| bytes", len(body), "| type", hd.get("Content-Type"),
          "| last-modified", hd.get("Last-Modified"))
    if body[:2] == b"PK":
        z = zipfile.ZipFile(io.BytesIO(body))
        for info in z.infolist():
            print(f"  member {info.filename}  {info.file_size:,} bytes")
        member = next((i.filename for i in z.infolist() if i.filename.lower().endswith(".csv")), None)
        if member:
            with z.open(member) as fh:
                text = io.TextIOWrapper(fh, encoding="utf-8-sig", errors="replace")
                rdr = csv.reader(text)
                header = next(rdr)
                print(f"\n  HEADER ({len(header)} cols):")
                print("  ", header)
                wanted = [c for c in header if re.match(
                    r"(TRACTFIPS|STCOFIPS|STATEABBRV|COUNTY|NRI_VER|RISK_SCORE|RISK_RATNG|"
                    r"(CFLD|RFLD|IFLD|WFIR|HRCN|TRND|HWAV|CWAV|WNTW|ERQK)_(RISKS|RISKR|AFREQ|EXPB))$", c)]
                for i, r in enumerate(rdr):
                    rec = dict(zip(header, r))
                    print("  ROW:", {k: rec.get(k) for k in wanted})
                    if i >= 2:
                        break
                n = 3 + sum(1 for _ in rdr)
                print(f"  total data rows: {n:,}")
        break

# ── Census ZCTA <-> tract relationship ──────────────────────────────
section("Census 2020 ZCTA relationship files")
base = "https://www2.census.gov/geo/docs/maps-data/data/rel2020/zcta520/"
st, hd, body = get(base)
print("status", st, "| files:", [h for h in hrefs(body) if h.endswith((".txt", ".csv", ".pdf"))])
for f in ("tab20_zcta520_tract20_natl.txt", "tab20_zcta520_county20_natl.txt"):
    st, hd, body = get(base + f, limit=4000)
    print(f"\n  --- {f}: status {st}, content-length {hd.get('Content-Length')}")
    for line in body.decode("utf-8-sig", "replace").splitlines()[:3]:
        print("   ", line)

print("\nPROBE DONE", flush=True)
sys.exit(0)

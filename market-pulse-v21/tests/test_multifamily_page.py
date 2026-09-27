"""/multifamily over real HTTP — the finder wired into the page.

Run:  python tests/test_multifamily_page.py      (exit 0 = all pass)
SKIPS (exit 0) if data/zips.db is absent.

zip_finder.py is pure and tested on its own. This file tests the GLUE: that
the route feeds the finder the right rows, that the funnel the page PRINTS
still adds up after the template has rendered it, that a filter in the URL
reaches the table, that hostile URLs degrade instead of 500, and that the
deal checker does not silently reset the board above it.

Starts a real uvicorn and talks to it with urllib, as the ops suites do —
fastapi.testclient needs httpx, which this repo deliberately does not ship.
"""
import html
import os
import re
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
DB = os.path.join(ROOT, "data", "zips.db")
if not os.path.exists(DB):
    print("SKIP — no data/zips.db")
    sys.exit(0)

import zip_finder as Z                                   # noqa: E402

_COUNT = 0
_FAILS = []


def check(cond, msg):
    global _COUNT
    _COUNT += 1
    if not cond:
        _FAILS.append(msg)


PORT = int(os.environ.get("MF_PAGE_TEST_PORT", "58311"))
BASE = f"http://127.0.0.1:{PORT}"
server = subprocess.Popen(
    [sys.executable, "-m", "uvicorn", "main:app", "--host", "127.0.0.1",
     "--port", str(PORT), "--log-level", "warning"],
    cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)


def stop():
    if server.poll() is None:
        server.terminate()
        try:
            server.wait(timeout=10)
        except subprocess.TimeoutExpired:
            server.kill()


def get(path):
    try:
        with urllib.request.urlopen(BASE + path, timeout=60) as r:
            return r.status, r.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace")


deadline = time.time() + 60
up = False
while time.time() < deadline:
    if server.poll() is not None:
        print("server died:\n", (server.stdout.read() if server.stdout else "")[-3000:])
        sys.exit(1)
    try:
        urllib.request.urlopen(BASE + "/map", timeout=3)
        up = True
        break
    except urllib.error.HTTPError:
        up = True
        break
    except Exception:
        time.sleep(0.5)
if not up:
    stop()
    print("FAIL — server never came up")
    sys.exit(1)


def _clean(x):
    return html.unescape(re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", x))).strip()


def funnel_numbers(page):
    """(start, [removed...], end) exactly as the page PRINTS them."""
    nums = [int(n.replace(",", "").replace("−", ""))
            for n in re.findall(r'<span class="fn-n">([^<]+)</span>', page)]
    return (nums[0], nums[1:-1], nums[-1]) if len(nums) >= 2 else (None, [], None)


def table_rows(page):
    if "<tbody>" not in page:
        return []
    body = page[page.index("<tbody>"):page.index("</tbody>")]
    out = []
    for tr in re.findall(r"<tr>(.*?)</tr>", body, re.S):
        tds = re.findall(r"<td[^>]*>(.*?)</td>", tr, re.S)
        sorts = re.findall(r'<td[^>]*data-sort="([^"]*)"', tr)
        flags = re.search(r"has (\d+) state-level structural flag", tr)
        out.append({"cells": [_clean(t) for t in tds], "sorts": sorts,
                    "flags": flags.group(1) if flags else None})
    return out


def col(page, name):
    """Index of a table column by its data-col attribute."""
    heads = re.findall(r'<th data-col="([^"]+)"', page)
    return heads.index(name) if name in heads else None


try:
    n_oh = sqlite3.connect(DB).execute(
        "select count(*) from zips where state='OH'").fetchone()[0]

    # ── the default page ────────────────────────────────────────────
    st, page = get("/multifamily?state=OH")
    check(st == 200, f"the default page serves (got {st})")
    start, removed, end = funnel_numbers(page)
    check(start == n_oh,
          f"THE FUNNEL STARTS FROM EVERY ZIP IN THE STATE ({n_oh}), not from "
          f"the ones that survived a WHERE clause the page never mentioned "
          f"(got {start})")
    check(start is not None and start - sum(removed) == end,
          f"AND THE NUMBERS THE PAGE PRINTS ADD UP: {start} − {sum(removed)} "
          f"should be {end}. The module's funnel balancing is not enough on "
          f"its own; a template that hid a non-zero stage would break it here")
    tiers = dict(sqlite3.connect(DB).execute("select zip, rent_tier from zips").fetchall())
    starred, seen = [], 0
    for q in ("state=OH&unknown=1&max_price=900000", "state=ALL&unknown=1&max_price=900000&preset=cashflow"):
        _, pg = get(f"/multifamily?{q}")
        cz, cr = col(pg, "zip"), col(pg, "rent")
        for r in table_rows(pg):
            seen += 1
            if r["cells"][cr].endswith("*") and tiers.get(r["cells"][cz]):
                starred.append(r["cells"][cz])
    check(seen > 100 and not starred,
          f"A RENT WITH A SOURCE IS NEVER STARRED AS IMPUTED — the value÷204 "
          f"arithmetic test starred 392 real Zillow rents that happened to land "
          f"near it ({starred[:5]})")
    # ── rent sources on the page ──
    _, hpage = get("/multifamily?state=OH&unknown=1&max_price=900000")
    cz, cr = col(hpage, "zip"), col(hpage, "rent")
    hrows = table_rows(hpage)
    inc = dict(sqlite3.connect(DB).execute(
        "select zip, median_household_income from zips").fetchall())
    by_tier = {}
    wrong, strain_wrong, n_strain = [], [], 0
    for r in hrows:
        z, cell = r["cells"][cz], r["cells"][cr]
        t = tiers.get(z)
        by_tier[t] = by_tier.get(t, 0) + 1
        tagged = cell.endswith("HUD") or cell.endswith("HUD !")
        if tagged != (t in ("safmr", "fmr")):
            wrong.append((z, t, cell))
        rent = int(re.sub(r"[^0-9]", "", cell.split("HUD")[0]) or 0)
        should = (t in ("safmr", "fmr") and bool(inc.get(z))
                  and rent * 12 / inc[z] >= 0.40)
        n_strain += should
        if cell.endswith("HUD !") != should:
            strain_wrong.append((z, rent, inc.get(z), cell))
    check(by_tier.get("safmr") and by_tier.get("fmr") and by_tier.get("zori") and not wrong,
          f"EVERY HUD-BASED RENT IS TAGGED HUD AND NO ZILLOW RENT IS — checked "
          f"row by row against zips.db ({by_tier}; wrong: {wrong[:3]})")
    check(n_strain and not strain_wrong,
          f"A HUD RENT AT 40%+ OF THE ZIP'S MEDIAN INCOME IS FLAGGED, and no "
          f"other is — checked against zips.db ({n_strain} flagged; wrong: "
          f"{strain_wrong[:3]})")
    for t, label in (("zori", "Zillow ZORI"), ("safmr", "HUD Small Area FMR"),
                     ("fmr", "HUD FMR (county)")):
        n = by_tier.get(t, 0)
        check(f"<b>{label}</b> on {n} row" in hpage,
              f"the legend counts {label} rows the way the table shows them ({n})")
    check("not an asking rent" in hpage and "1.01x" in hpage,
          "and a HUD tag's hover says what the figure is and how it compared "
          "with Zillow's")
    gaps = dict(sqlite3.connect(DB).execute(
        "select state, sum(median_rent_monthly is null) from zips "
        "where population >= 1500 group by state").fetchall())
    check(("no measured rent (Zillow or HUD)" in page) == bool(gaps.get("OH")),
          f"Ohio's rent step shows exactly when it removes something "
          f"({gaps.get('OH')} Ohio ZIPs without a rent)")
    gap_st = max(gaps, key=lambda k: gaps[k] or 0)
    _, gpage = get(f"/multifamily?state={gap_st}")
    check(gaps[gap_st] and "no measured rent (Zillow or HUD)" in gpage,
          f"and where ZIPs still have no rent ({gaps[gap_st]} in {gap_st}) the "
          f"gap is named, with the sources it was looked for in, instead of "
          f"vanishing silently as it used to")
    check(len(table_rows(page)) == end or (end > 100 and len(table_rows(page)) == 100),
          "the table shows what the funnel says is on the board (top 100)")

    # ── what is and isn't offered ───────────────────────────────────
    for proxy in ("walk_score", "restaurant_score", "crime_index"):
        check(f'name="{proxy}"' not in page and f'name="min_{proxy}"' not in page,
              f"{proxy} is not offered as a filter")
    check('name="min_income"' in page and 'name="min_degree"' in page
          and 'name="max_cost"' in page and 'name="area"' in page,
          "the always-backed filters are offered")
    has_renter = sqlite3.connect(DB).execute(
        "select count(pct_renter_occupied) from zips where state='OH'").fetchone()[0]
    if not has_renter:
        check('name="min_renter"' not in page and "Renter share" in page,
              "RENTER SHARE IS LISTED AS UNAVAILABLE, NOT OFFERED, while the "
              "Census column is empty — offered, it would mark every row "
              "no-data and empty the board")
    for p in Z.PENDING:
        check(html.escape(p["label"], quote=False) in page,
          f"'{p['label']}' is visible as not available yet")

    # Implicit submission: pressing Enter submits the FIRST submit button in
    # the form. If that were a preset, Enter would switch presets.
    form = page[page.index('<form class="finder"'):]
    first_submit = re.search(r'<button[^>]*type="submit"[^>]*>', form).group(0)
    check('name="use_preset"' not in first_submit,
          "PRESSING ENTER APPLIES THE FILTERS. The first submit button in "
          "the form is the plain Apply, not a preset — otherwise Enter in the "
          "price box would silently switch the ranking to 'Balanced'")

    # ── filters reach the table ─────────────────────────────────────
    q = "/multifamily?state=OH&unknown=1&min_income=75000&area=suburban&area=urban"
    st, page = get(q)
    check(st == 200, "a filtered page serves")
    start, removed, end = funnel_numbers(page)
    check(start - sum(removed) == end, "the filtered funnel adds up too")
    ci, ct = col(page, "income"), col(page, "type")
    rows = table_rows(page)
    check(rows and all(float(r["sorts"][ci]) >= 75_000 for r in rows),
          "EVERY ROW ON THE BOARD MEETS THE INCOME FILTER — read back off the "
          "rendered table, not the module")
    check(rows and all(r["cells"][ct] in ("Suburban", "Urban") for r in rows),
          "and the area filter: no rural ZIP survives")
    check("your filter" not in page or "household income below $75,000" in page,
          "the income filter appears as its own funnel step")

    st, page = get("/multifamily?state=OH&unknown=1&max_cost=0")
    rows = table_rows(page)
    chh = col(page, "hh")
    check(all(float(r["sorts"][chh]) <= 0 for r in rows),
          "'Tenants cover it' keeps only rows whose cost after rent is zero or "
          "better — ZERO IS A THRESHOLD, not 'off'")
    check("above $0/mo" in page, "and it shows up in the funnel")

    # ── the form re-renders what you chose ──────────────────────────
    # Otherwise the NEXT Apply submits the dropdown's default and silently
    # turns the filter off — worst for zero thresholds, which are falsy.
    def selected(page, name):
        m = re.search(rf'<select name="{name}"[^>]*>(.*?)</select>', page, re.S)
        return re.findall(r'<option value="([^"]*)"\s*selected', m.group(1)) if m else None
    check(selected(page, "max_cost") == ["0"],
          "AFTER CHOOSING 'Tenants cover it' ($0), THAT OPTION IS STILL "
          "SELECTED — a truthiness check would show 'Any' and the next Apply "
          f"would drop the filter (got {selected(page, 'max_cost')})")
    st, page = get("/multifamily?state=OH&min_income=75000&min_trend=0&w_growth=2")
    check(selected(page, "min_income") == ["75000"],
          f"exactly one income option is selected, the one chosen "
          f"(got {selected(page, 'min_income')})")
    check(selected(page, "min_trend") == ["0"], "'not falling' (0) stays selected")
    check(selected(page, "min_degree") == [], "an unset filter selects nothing, so 'Any' shows")
    check(selected(page, "w_growth") == ["2"], "a chosen weight stays selected")

    # ── ordering ────────────────────────────────────────────────────
    st, page = get("/multifamily?state=OH&unknown=1")
    rows = table_rows(page)
    cs = col(page, "safety")
    flags = ["unverified" in r["cells"][cs].lower() for r in rows]
    first_unverified = flags.index(True) if True in flags else len(flags)
    check(not any(flags[:first_unverified][i] for i in range(first_unverified))
          and all(flags[first_unverified:]),
          "ON THE RENDERED BOARD every verified-safe row sits above every "
          "unverified one")

    # ── presets ─────────────────────────────────────────────────────
    boards = {}
    for key in Z.PRESETS:
        st, page = get(f"/multifamily?state=OH&unknown=1&use_preset={key}")
        check(st == 200, f"preset {key} serves")
        check(re.search(rf'value="{key}" class="preset-btn on"', page) is not None,
              f"preset {key} is shown as selected")
        boards[key] = [r["cells"][1] for r in table_rows(page)[:10]]
    check(len({tuple(v) for v in boards.values()}) == len(boards),
          "EVERY PRESET PRODUCES A DIFFERENT TOP 10. If two presets agreed, "
          "one of them would be a label on the same ranking")

    # ── hostile and junk URLs degrade, never 500 ────────────────────
    for bad in ("min_income=76000&min_cap=99&area=castle&w_cashflow=9",
                "safetier=unknown", "max_cost=-1&min_trend=inf",
                "w_cashflow=0&w_yield=0&w_growth=0&w_income=0&w_education=0",
                "area=&min_income=&use_preset=", "min_renter=40&min_multi=25",
                "state=ZZ", "max_price=abc&down_pct=nan&units=9"):
        st, page = get(f"/multifamily?state=OH&{bad}")
        check(st == 200, f"junk URL ({bad}) serves 200, not {st}")
    st, page = get("/multifamily?state=OH&min_cap=99")
    check('value="99"' not in page and "above 99" not in page and "below 99" not in page,
          "a threshold the page never offered is not honoured or echoed")
    st, page = get("/multifamily?state=OH&w_cashflow=0&w_yield=0&w_growth=0"
                   "&w_income=0&w_education=0&unknown=1")
    check("ranked on the Balanced preset" in page,
          "all priorities set to Ignore says it fell back to Balanced, "
          "rather than silently ranking on something")
    if not has_renter:
        st, page = get("/multifamily?state=OH&unknown=1&min_renter=40")
        start, removed, end = funnel_numbers(page)
        st2, page2 = get("/multifamily?state=OH&unknown=1")
        check(end == funnel_numbers(page2)[2],
              "A URL ASKING FOR AN UNAVAILABLE CENSUS FILTER IS IGNORED — it "
              "does not empty the board by marking every row no-data")

    # ── the empty board names what emptied it ───────────────────────
    st, page = get("/multifamily?state=OH&unknown=1&min_cap=7&min_income=125000")
    check("The step that emptied the board" in page and "one of your filters" in page,
          "an empty board names the step that emptied it, and says it was "
          "one of the user's own filters")
    check("What it would take" not in page,
          "and does NOT suggest raising the budget, which would not help")

    # ── the deal checker keeps the board ────────────────────────────
    st, page = get("/multifamily?state=OH&unknown=1&min_income=75000&area=urban"
                   "&use_preset=growth")
    deal = page[page.index('<form class="deal-form"'):]
    deal = deal[:deal.index("</form>")]
    for name, value in (("min_income", "75000"), ("area", "urban"), ("w_growth", "3")):
        check(f'name="{name}" value="{value}"' in deal,
              f"THE LISTING CHECKER CARRIES {name}={value}. Without it, "
              f"checking a listing would silently reset the filters above it")
    st, page = get("/multifamily?state=OH&unknown=1&min_income=75000&area=urban"
                   "&w_cashflow=1&w_yield=0&w_growth=3&w_income=1&w_education=1"
                   "&check_price=250000&check_rent=1400")
    check(st == 200 and "deal-verdict" in page,
          "and running a listing with those carried params still serves a verdict")
    check("household income below $75,000" in page,
          "with the income filter still applied to the board")

    # ── national ────────────────────────────────────────────────────
    import json as _json
    n_all = sqlite3.connect(DB).execute(
        "select count(*) from zips where state is not null and state != ''").fetchone()[0]
    st, page = get("/multifamily?state=ALL&unknown=1")
    check(st == 200, f"the national board serves (got {st})")
    start, removed, end = funnel_numbers(page)
    check(start == n_all and start - sum(removed) == end,
          f"THE NATIONAL FUNNEL STARTS FROM EVERY ZIP IN THE COUNTRY ({n_all:,}) "
          f"AND ADDS UP (got {start} − {sum(removed)} vs {end})")
    check('value="ALL" selected' in page, "'All states' stays selected")
    rows = table_rows(page)
    cst = col(page, "state")
    states_seen = {r["cells"][cst] for r in rows} if cst is not None else set()
    check(cst is not None and len(states_seen) >= 5,
          f"the national table has a State column and spans many states ({len(states_seen)})")
    check("Your scenario:" not in page,
          "NO SCENARIO CARD NATIONALLY — _fha_piti('ALL') quietly returns default "
          "rates instead of failing, so nothing else stops a card priced for a "
          "state that doesn't exist")
    check("Pick a state for the scenario" in page and 'class="deal-form"' not in page,
          "THE SCENARIO CARD AND LISTING CHECKER ASK FOR A STATE instead of pricing "
          "one building with a blend of states' tax and insurance rates")

    verified_states = {r["cells"][cst] for r in rows
                       if "unverified" not in r["cells"][col(page, "safety")].lower()}
    check(len(verified_states) >= 3,
          f"VERIFIED-SAFE ROWS COME FROM SEVERAL STATES ({sorted(verified_states)}). "
          f"Look up every row's safety under one state and only that state's "
          f"cities verify — every other row turns 'unverified' and sinks, which "
          f"a row-by-row comparison can miss when it samples the genuinely "
          f"unverified ones")

    # The invariant: a ZIP's numbers on the national board are exactly its
    # numbers on its own state's board. Anything else means a row was priced
    # with another state's tax, insurance or safety record.
    chh, csc = col(page, "hh"), col(page, "safety")
    czip = col(page, "zip")
    # ONE ROW FROM EACH OF SEVERAL STATES. Sampling the top rows alone let a
    # bug through: price every row as one state and the other states' rows
    # sink as "unverified", so the top of the board was all one state and
    # agreed with itself. Capped rows are preferred, since the veto depends
    # on each state's structural flags (Ohio has none, Florida three).
    by_state = {}
    for r in sorted(rows, key=lambda r: "capped" not in r["cells"][col(page, "score")]):
        by_state.setdefault(r["cells"][cst], r)
    sample = list(by_state.values())[:8]
    mismatches = []
    for r in sample:
        z, stt = r["cells"][czip], r["cells"][cst]
        st2, p2 = get(f"/multifamily?state={stt}&unknown=1")
        match = [x for x in table_rows(p2) if x["cells"][czip] == z]
        if not match:
            mismatches.append((z, stt, "absent from its state board"))
            continue
        m2 = match[0]
        c2hh, c2sc = col(p2, "hh"), col(p2, "safety")
        cscore, c2score = col(page, "score"), col(p2, "score")
        capped1 = "capped" in r["cells"][cscore]
        capped2 = "capped" in m2["cells"][c2score]
        if (r["sorts"][chh] != m2["sorts"][c2hh]
                or r["cells"][csc] != m2["cells"][c2sc] or capped1 != capped2
                or r["flags"] != m2["flags"]):
            mismatches.append((z, stt, r["sorts"][chh], m2["sorts"][c2hh]))
    check(not mismatches,
          f"EVERY SAMPLED ZIP HAS THE SAME COST AFTER RENT, THE SAME SAFETY "
          f"RECORD AND THE SAME TRAJECTORY VETO NATIONALLY AS ON ITS OWN STATE'S "
          f"BOARD — each row is priced and flagged with its own state's rates "
          f"({mismatches})")
    link = re.search(r'<a href="(/multifamily\?state=[A-Z]{2}[^"]*)"[^>]*title="Open the', page)
    check(link and "unknown=1" in html.unescape(link.group(1)),
          "and a row's state links to that state's board with the filters kept")

    # ── weather and hazards ─────────────────────────────────────────
    hz_file = _json.load(open(os.path.join(ROOT, "data", "zip_hazards.json")))
    hz, hz_med = hz_file["zips"], hz_file["_meta"]["median"]
    cl = _json.load(open(os.path.join(ROOT, "data", "zip_climate.json")))["zips"]
    st, page = get("/multifamily?state=ALL&unknown=1&min_winter=20&max_summer=90"
                   "&max_snow=30&max_flood=150&max_quake=50")
    rows = table_rows(page)
    zips_shown = [r["cells"][col(page, "zip")] for r in rows]
    check(rows and all(cl[z]["wl"] >= 20 and cl[z]["sh"] <= 90 and cl[z].get("sn", 99) <= 30
                       for z in zips_shown),
          "EVERY ROW MEETS THE WEATHER FILTERS — checked against the NOAA file, "
          "not the page's own rendering")
    check(rows and all(hz[z]["fl"] <= 150 and hz[z]["eq"] <= 50 for z in zips_shown),
          "and the hazard filters, in dollars, checked against the FEMA file")
    for label in ("winter average low below 20°F", "summer average high above 90°F",
                  "flood damage above $150/yr per $100k"):
        check(label in page, f"the funnel names '{label}'")
    ch = col(page, "hazard")
    bad_total = [z for r, z in zip(rows, zips_shown)
                 if abs(float(r["sorts"][ch]) - sum(hz[z].values())) > 0.02]
    check(rows and not bad_total,
          f"THE HAZARDS COLUMN IS THE FOUR FEMA DOLLAR FIGURES ADDED UP, for every "
          f"row — not a rank, not the worst one alone ({bad_total[:3]})")
    check(rows and all(r["cells"][ch].startswith("$") or r["cells"][ch].startswith("under $1")
                       for r in rows) and "/100<" not in page,
          "and it reads in dollars, with no x/100 score anywhere")
    typical = "The typical U.S. ZIP: flood ${:,.0f}".format(hz_med["flood"])
    check(typical in page and "wildfire under $1" in page,
          "THE HAZARD FILTERS SAY WHAT A TYPICAL ZIP EXPECTS, from the data file's "
          "own national medians — a threshold means nothing without it")
    names = {"fl": "flood", "wf": "wildfire", "wd": "hurricane & tornado", "eq": "earthquake"}
    flagged, plain = [], []
    for stt in ("LA", "OH"):             # hurricane country, and not
        st, page = get(f"/multifamily?state={stt}&unknown=1")
        ch, cz = col(page, "hazard"), col(page, "zip")
        for r in table_rows(page):
            r["hz"], r["hzcell"] = hz[r["cells"][cz]], r["cells"][ch]
            (flagged if sum(r["hz"].values()) >= 250 else plain).append(r)
    check(flagged and all(r["hzcell"].endswith(names[max(r["hz"], key=r["hz"].get)])
                          for r in flagged),
          f"A FLAGGED ROW ($250+) NAMES ITS BIGGEST HAZARD, from the FEMA file "
          f"({len(flagged)} flagged in LA and OH)")
    check(plain and all(re.fullmatch(r"(\$[\d,]+|under \$1)", r["hzcell"]) for r in plain),
          "and an ordinary row shows just the dollars — flood is the baseline "
          "almost everywhere, so naming it on every row would be noise")
    check('name="max_flood"' in page and 'name="min_winter"' in page,
          "weather and hazard filters are offered nationally")

    st, page = get("/multifamily?state=CT&unknown=1")
    check("<legend>Natural hazards</legend>" not in page,
          "and no empty 'Natural hazards' heading with nothing under it")
    check('name="max_flood"' not in page and 'name="min_winter"' in page,
          "CONNECTICUT GETS WEATHER FILTERS BUT NO HAZARD FILTERS — FEMA's tracts "
          "use CT's 2022 renumbering, the ZIP file the old one, so there are no "
          "hazard figures to filter on")
    check("Flood damage" in page and "No FEMA hazard figures for CT" in page,
          "and says so: the hazard filters show as unavailable for CT")
    e_plain = funnel_numbers(page)[2]
    st, page = get("/multifamily?state=CT&unknown=1&max_flood=100")
    check(funnel_numbers(page)[2] == e_plain,
          "A URL ASKING FOR A FLOOD FILTER IN CT IS IGNORED — it cannot empty "
          "the board by marking every row no-data")

    # ── nothing else broke ──────────────────────────────────────────
    for path in ("/map", "/norcal", "/fcf-quality"):
        st, _ = get(path)
        check(st == 200, f"{path} still serves (got {st})")
finally:
    stop()


if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for m in _FAILS:
        print("  ✗", m)
    sys.exit(1)
print(f"OK — all {_COUNT} multifamily-page checks passed.")
print("   Over real HTTP: the printed funnel adds up, filters reach the table,\n"
      "   presets differ, junk URLs degrade, and the listing checker keeps the board.")
sys.exit(0)

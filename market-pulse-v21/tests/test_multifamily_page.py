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
        out.append({"cells": [_clean(t) for t in tds], "sorts": sorts})
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
    check("no measured rent (Zillow ZORI)" in page,
          "the rent gap — two thirds of Ohio — is named on the page instead "
          "of vanishing silently as it used to")
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

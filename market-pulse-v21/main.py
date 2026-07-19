"""Market Pulse — Real Estate & Finance Dashboard."""
import asyncio, hmac, json, os, logging, sqlite3
from contextlib import asynccontextmanager
from datetime import date
from pathlib import Path
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import JSONResponse, RedirectResponse, Response
from fastapi.middleware.gzip import GZipMiddleware
from dotenv import load_dotenv
from data_providers import STATES, MORTGAGE_30Y_RATE, qualifying_income
from sec_edgar import build_net_net_screener
from state_neighborhoods import get_state_neighborhoods, STATE_METROS
from database import (init_db, save_price, save_prices_bulk, get_all_prices, delete_price,
                      lock_portfolio, update_portfolio_prices, exit_holding,
                      close_portfolio, get_all_portfolios,
                      add_user, get_user_count, list_users)

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _fmt_obs_date(iso: str) -> str:
    """Format an ISO date like '2026-05-01' as 'May 1, 2026' for the
    rate chip + affordability tooltip. Falls back to the raw string on
    parse failure so a malformed value never breaks the page."""
    try:
        d = date.fromisoformat(iso)
        # Build "May 1, 2026" without the Linux-only %-d flag so this
        # also works on Windows dev machines.
        return f"{d.strftime('%b')} {d.day}, {d.year}"
    except (ValueError, TypeError):
        return iso


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Market Pulse starting up...")
    init_db()
    try:
        from crm import maybe_seed, maybe_seed_templates
        maybe_seed()
        maybe_seed_templates()
    except Exception as e:
        logger.warning("CRM seed skipped: %s", e)
    yield
    logger.info("Market Pulse shutting down.")


app = FastAPI(title="Market Pulse", lifespan=lifespan)

# Compress responses ≥1KB. HTML pages average 70-130KB and inline JSON
# payloads compress to ~25-35% of original size — material wire savings
# without any code changes elsewhere. Skipped for already-compressed
# content types (images, gzipped GeoJSON in the future, etc.) by the
# middleware itself based on Content-Type.
app.add_middleware(GZipMiddleware, minimum_size=1024)


# StaticFiles subclass that adds long-cache headers to every response.
# /static/ holds files that change only on deploy (CSS, GeoJSON,
# vendored libs). Browsers will reuse cached copies aggressively
# instead of refetching every navigation. Filename-based cache busting
# is the user's responsibility if they edit a file (rare for /static).
class CachedStaticFiles(StaticFiles):
    async def get_response(self, path: str, scope):
        response = await super().get_response(path, scope)
        # 1 day for HTML/JSON-ish files (data may refresh server-side);
        # 1 year for images and font assets (effectively immutable).
        if isinstance(response, Response):
            response.headers["Cache-Control"] = "public, max-age=86400"
        return response


static_dir = os.path.join(os.path.dirname(__file__) or ".", "static")
if os.path.isdir(static_dir):
    app.mount("/static", CachedStaticFiles(directory=static_dir), name="static")
templates = Jinja2Templates(directory="templates")


@app.get("/")
async def home():
    """Map-first landing — the national overview is the primary view; the
    other tools (state data, affordability, screener, results) are one
    click away in the sidebar. Permanent redirect so search engines and
    bookmarks settle on /map as the canonical home URL."""
    return RedirectResponse(url="/map", status_code=308)


@app.get("/map")
async def national_map(request: Request):
    """National overview — every supported metro pinned on a single Leaflet
    map. Each pin's color reflects the top ZIP's composite score under the
    default investor persona; click → popup with metro summary + link to
    the full per-metro deep-dive at /real-estate/{slug}/map."""
    # State-level lookups used by the find-your-fit matcher to boost
    # profiles toward states with the right climate / tax / growth
    # characteristics — without needing per-metro hand-curated data.
    from data_providers import (
        STATE_SUNSHINE_DAYS, STATE_INCOME_TAX_EFFECTIVE, STATE_POPULATION_GROWTH,
        CHOROPLETH_STATES, CHOROPLETH_METRICS,
        MORTGAGE_30Y_RATE, MORTGAGE_30Y_OBS_DATE,
        qualifying_income,
    )
    from structural import (state_structural, state_trajectories,
                            apply_trajectory_veto, TRAJECTORY_BADGES)
    # Statewide value trajectories (median of each state's ZIP-level
    # 60-month ZHVI series; lru-cached after the first render).
    state_trajs = state_trajectories()
    metros = []
    for slug, cfg in STATE_METROS.items():
        # Stubs go through the same get_state_neighborhoods path as
        # real metros — get_state_neighborhoods synthesizes a single
        # virtual ZIP for stubs from CHOROPLETH_STATES and runs it
        # through compute_zip_metrics, so composite scoring is on the
        # same scale for everything. is_stub flag flows through so the
        # popup can still mark these as state-level estimates.
        data = get_state_neighborhoods(slug)
        if not data:
            continue
        zips = data.get("neighborhoods", [])
        if not zips:
            continue
        top_zip = zips[0]  # already sorted by composite_score desc
        n = len(zips)
        avg_score = sum(z["composite_score"] for z in zips) / n
        avg_cap = sum(z["cap_rate_pct"] for z in zips) / n
        avg_home = sum(z["median_home_value"] for z in zips) / n
        avg_rent = sum(z["median_rent_monthly"] for z in zips) / n
        # Aggregate the lifestyle/quality dimensions from per-ZIP data.
        avg_walk = sum(z.get("walk_score", 0) for z in zips) / n
        avg_school = sum(z.get("pct_bachelors", 0) for z in zips) / n
        avg_crime = sum(z.get("crime_index", 50) for z in zips) / n
        avg_restaurants = sum(z.get("restaurant_score", 0) for z in zips) / n
        # Per-persona metro composites (avg of per-ZIP composite_by_persona).
        # Lets the right-rail leaderboard re-sort instantly when the user
        # toggles persona, without any refetch.
        persona_keys = ('balanced', 'investor', 'lifestyle')
        composite_by_persona = {}
        for pkey in persona_keys:
            vals = [z.get('composite_by_persona', {}).get(pkey) for z in zips]
            vals = [v for v in vals if v is not None]
            composite_by_persona[pkey] = round(sum(vals) / len(vals), 1) if vals else round(avg_score, 1)
        # Structural lens: state regime flags + statewide value
        # trajectory. A declining/decelerating trajectory caps the
        # persona composites (visibly — the sidebar shows why) so
        # momentum can't carry a structurally-shifting market to the
        # top of the leaderboard.
        sst = state_structural(cfg["state"])
        straj = state_trajs.get(cfg["state"])
        struct_capped = False
        for pkey in persona_keys:
            adj, vetoed = apply_trajectory_veto(
                composite_by_persona[pkey],
                straj["label"] if straj else None,
                len(sst["flags"]))
            composite_by_persona[pkey] = adj
            struct_capped = struct_capped or vetoed
        structural_info = {
            "chips": sst["chips"],
            "traj": straj,
            "traj_badge": TRAJECTORY_BADGES.get(straj["label"]) if straj else None,
            "capped": struct_capped,
        }
        # State-level lookups for tax-haven + growth-momentum signals.
        sunshine = STATE_SUNSHINE_DAYS.get(cfg["state"], 200)
        state_income_tax = STATE_INCOME_TAX_EFFECTIVE.get(cfg["state"], 0.045)
        state_pop_growth = STATE_POPULATION_GROWTH.get(cfg["state"], 0.5)
        # Salary needed to qualify for the metro's median home — 20% down,
        # 30Y fixed at the current rate, 28% front-end DTI on full PITI.
        # Same methodology as the affordability page so numbers stay in sync.
        qual_income = qualifying_income(avg_home, cfg["state"], MORTGAGE_30Y_RATE)
        metros.append({
            "slug": slug,
            "state": cfg["state"],
            "metro_label": cfg["metro_label"],
            "map_center": cfg["map_center"],
            "tiktok_hashtag": cfg.get("tiktok_hashtag"),     # TikTok hashtag for popup CTA
            "instagram_hashtag": cfg.get("instagram_hashtag"),  # Instagram hashtag for popup CTA
            # is_stub now comes from get_state_neighborhoods (which
            # downgrades to False when a stub was promoted from real
            # zips.db members). Falls back to the cfg flag if data
            # didn't bubble it up — preserves old behavior on misses.
            "is_stub": bool(data.get("is_stub", cfg.get("is_stub"))),
            "top_zip": {
                "zip": top_zip["zip"],
                "name": top_zip["name"],
                "composite_score": top_zip["composite_score"],
                "cap_rate_pct": top_zip["cap_rate_pct"],
            },
            "zip_count": n,
            "avg_composite": round(avg_score, 1),
            "composite_by_persona": composite_by_persona,
            "structural": structural_info,
            "avg_cap_rate_pct": round(avg_cap, 2),
            "avg_home_value": round(avg_home),
            "avg_rent": round(avg_rent),
            "qualifying_income": qual_income,
            "avg_walk_score": round(avg_walk, 1),
            "avg_pct_bachelors": round(avg_school, 1),
            "avg_crime_index": round(avg_crime, 1),
            "avg_restaurant_score": round(avg_restaurants, 1),
            "sunshine_days": sunshine,
            "state_income_tax_pct": round(state_income_tax * 100, 2),
            "state_pop_growth_pct": round(state_pop_growth, 2),
        })
    metros.sort(key=lambda m: -m["avg_composite"])
    # Build a FIPS→state-data map for the choropleth. The GeoJSON's
    # feature.id is the FIPS code, so the client looks it up by FIPS.
    # We pass through every metric key the sidebar might color by;
    # CHOROPLETH_METRICS is the source of truth. has_metros lets the
    # client highlight states we cover with metros.
    states_with_metros = {cfg["state"] for cfg in STATE_METROS.values()}
    # Derived structural metrics for the two new choropleth layers.
    # Computed here (not in data_providers) because insurance_burden
    # must use the OVERRIDDEN home_value and value_momentum needs
    # zips.db — both are only settled at request time. Idempotent.
    for code, sd in CHOROPLETH_STATES.items():
        hv, ins = sd.get("home_value"), sd.get("insurance")
        sd["insurance_burden"] = round(ins / hv * 100, 2) if ins and hv else None
        t = state_trajs.get(code)
        sd["value_momentum"] = t["decel_pct"] if t else None
    metric_keys = [m["key"] for m in CHOROPLETH_METRICS]
    choropleth_by_fips = {}
    for code, sd in CHOROPLETH_STATES.items():
        entry = {
            "code": code,
            "name": sd["name"],
            "has_metros": code in states_with_metros,
        }
        for k in metric_keys:
            if k in sd:
                entry[k] = sd[k]
        choropleth_by_fips[sd["fips"]] = entry
    return templates.TemplateResponse("national_map.html", {
        "request": request,
        "metros": metros,
        "stub_count": sum(1 for m in metros if m.get("is_stub")),
        "choropleth_states": choropleth_by_fips,
        "choropleth_metrics": CHOROPLETH_METRICS,
        "mortgage_30y_rate": MORTGAGE_30Y_RATE,
        "mortgage_30y_obs_date": _fmt_obs_date(MORTGAGE_30Y_OBS_DATE),
    })


@app.get("/real-estate")
async def real_estate():
    """Permanent redirect to /map. The standalone State Data dashboard
    was retired once the country → state → metro drill-down landed on
    /map (Phase 1, P96): state pills became the choropleth, the
    Goldilocks rankings became the State Info card's persona row, and
    the FRED-driven metric cards were already duplicated in the /map
    sidebar. /real-estate/{slug}/map (per-metro deep-dive) stays."""
    return RedirectResponse(url="/map", status_code=308)


@app.get("/real-estate/{slug}/map")
async def state_map(request: Request, slug: str):
    """ZIP-level investor map for a metro. `slug` is a state code (e.g. 'TX')
    for the state's primary metro, or a `{state}-{tag}` slug for a secondary
    one (e.g. 'UT-STG'). Data is rendered server-side — no fetch."""
    data = get_state_neighborhoods(slug)
    if data is None:
        return JSONResponse(
            {"error": f"No metro deep-dive available for slug '{slug}'."},
            status_code=404,
        )
    state_code = data["state"]
    state_name = STATES.get(state_code, {}).get("name", state_code)
    return templates.TemplateResponse("state_map.html", {
        "request": request,
        "state": state_code,
        "state_name": state_name,
        "slug": data["slug"],
        "data": data,
    })


# 8 states the affordability page hand-curates (with bracket detail,
# property tax caps, homestead exemptions). The other 43 fall back to
# simplified defaults synthesized from CHOROPLETH_STATES.
_AFFORDABILITY_HAND_CURATED = {"NV", "CA", "UT", "TX", "AZ", "FL", "GA", "IN"}


@app.get("/affordability")
async def affordability(request: Request):
    from data_providers import (
        MORTGAGE_30Y_RATE, MORTGAGE_30Y_OBS_DATE, CHOROPLETH_STATES,
        TAX_DATA_AS_OF,
    )
    # Synthesize a simplified config for the 43 states that aren't in
    # the template's hand-curated STATE_DATA. Each gets a flat-rate
    # bracket from CHOROPLETH_STATES.income_tax, an effective property
    # tax rate from .property_tax, and an insurance multiplier derived
    # from insurance / home_value. No bracket detail, no caps, no
    # homestead exemption — just a directionally-correct default so the
    # comparison table shows all 51 states instead of 8.
    state_defaults: dict[str, dict] = {}
    for code, sd in CHOROPLETH_STATES.items():
        if code in _AFFORDABILITY_HAND_CURATED:
            continue   # template's STATE_DATA wins for these
        prop_tax = sd.get("property_tax")        # already in % form (e.g. 0.40 means 0.40%)
        income_tax = sd.get("income_tax")        # already in % form
        insurance = sd.get("insurance")          # annual $ amount
        home_value = sd.get("home_value")
        if prop_tax is None or income_tax is None or not home_value:
            continue
        ins_mult = (insurance / home_value) if (insurance and home_value > 0) else 0.0035
        state_defaults[code] = {
            "name": sd.get("name", code),
            "propertyTaxEffective": round(prop_tax / 100.0, 5),
            "propertyTaxCap": None,
            # Single flat-rate bracket; threshold None means unbounded
            # (handled by calculateStateIncomeTax).
            "stateIncomeTaxRates": [{"rate": round(income_tax / 100.0, 5), "threshold": None}],
            "insuranceMultiplier": round(ins_mult, 5),
            "primaryResidenceDiscount": False,
            "homesteadExemption": 0,
            "_isDefault": True,   # tag so the table can mark approximate rows
            "notes": "",
        }
    return templates.TemplateResponse("affordability.html", {
        "request": request,
        "mortgage_30y_rate": MORTGAGE_30Y_RATE,
        "mortgage_30y_obs_date": _fmt_obs_date(MORTGAGE_30Y_OBS_DATE),
        "state_defaults": state_defaults,
        "tax_data_as_of": TAX_DATA_AS_OF,
    })


@app.get("/finance")
async def finance(request: Request):
    # Public. Was admin-gated when the only view was a live SEC EDGAR
    # screen (admins running fresh fetches at will); now that the
    # monthly snapshot system makes the page mostly read-only, no
    # reason to hide it. /results (paper portfolio) is still admin.
    return templates.TemplateResponse("finance.html", {"request": request})


@app.get("/lynch")
async def lynch(request: Request):
    """Peter Lynch GARP screener — large-cap value growth.
    Reads from data/lynch_snapshots/ (monthly cron-built)."""
    return templates.TemplateResponse("lynch.html", {"request": request})


@app.get("/norcal")
async def norcal_page(request: Request, assets: float = 200_000,
                      reserves: float = 40_000, down_pct: float = 20.0,
                      region: str = "All CA",
                      zip: str = "", price: float = 0, sqft: float = 0,
                      income: float = 0):
    """California Real Estate — statewide strict screen (5 quality gates
    + budget gate) with region pills and a per-listing deal checker."""
    from norcal import screen, deal_check, REGIONS
    assets = max(0.0, min(50_000_000, assets))
    reserves = max(0.0, min(assets, reserves))
    down_pct = max(3.0, min(100.0, down_pct))
    if region not in REGIONS:
        region = "All CA"
    res = await asyncio.to_thread(screen, assets, reserves, down_pct,
                                  33.0, 45.0, 30.0, region)
    check = None
    if zip and price > 0:
        check = await asyncio.to_thread(
            deal_check, zip.strip(), price, sqft or None, income or None,
            assets, reserves, down_pct)
    return templates.TemplateResponse("norcal.html", {
        "request": request, "res": res, "power": res["power"], "check": check,
        "regions": REGIONS,
    })


@app.get("/value-add")
async def value_add_page(request: Request, region: str = "All CA",
                         zip: str = "", price: float = 0, units: int = 2,
                         rehab: float = 0, rent: float = 0, income: float = 0):
    """CA needs-work multifamily finder: hunting-ground ZIP ranking +
    203(k)-first rehab underwriting. See value_add.py."""
    from value_add import hunting_grounds, rehab_check
    from norcal import REGIONS
    if region not in REGIONS:
        region = "All CA"
    res = await asyncio.to_thread(hunting_grounds, region)
    check = None
    if zip and price > 0:
        check = await asyncio.to_thread(
            rehab_check, zip.strip(), price, int(units), max(0.0, rehab),
            rent or None, income or None)
    return templates.TemplateResponse("value_add.html", {
        "request": request, "res": res, "check": check, "regions": REGIONS,
    })


@app.get("/landscaper")
async def landscaper_page(request: Request, book: str = ""):
    """Bilingual (ES-first) Bay Area landscaping pricing tool: ZIP wealth
    tiers → suggested prices, sqft quotes, cost calculator, client book,
    day-route clustering, before/after. See landscaper.py.

    ?book=<code> connects a shared server-side book: the unguessable
    code IS the auth (same pattern as prototype feedback tokens). No
    code → on-device (localStorage) only, as before."""
    from landscaper import bay_pricing
    from database import landscaper_book_exists
    pricing = await asyncio.to_thread(bay_pricing)
    book = (book or "").strip()
    book_valid = bool(book) and await asyncio.to_thread(landscaper_book_exists, book)
    return templates.TemplateResponse("landscaper.html", {
        "request": request, "pricing": pricing,
        "book": book if book_valid else "",
    })


@app.get("/jardin", include_in_schema=False)
async def jardin_alias(request: Request, book: str = ""):
    """Short, textable Spanish alias for the landscaper tool."""
    dest = "/landscaper" + (f"?book={book.strip()}" if book.strip() else "")
    return RedirectResponse(dest, status_code=302)


# ── Shared landscaper book API ──────────────────────────────────────
# Book creation is admin-only (you make a book, text him the ?book=CODE
# link). Every read/write requires a valid code — the code is the auth.

@app.post("/api/landscaper/books")
async def api_landscaper_create_book(request: Request):
    """Admin: create a shared book. Body: {name}. Returns {code}."""
    gate = _admin_gate(request)
    if gate:
        return gate
    body = await request.json()
    from database import landscaper_create_book
    code = await asyncio.to_thread(landscaper_create_book,
                                   (body.get("name") or "Book").strip())
    if not code:
        return JSONResponse({"error": "could not create book"}, status_code=500)
    return JSONResponse({"code": code})


@app.get("/api/landscaper/books")
async def api_landscaper_list_books(request: Request):
    """Admin: list all books for the management view."""
    gate = _admin_gate(request)
    if gate:
        return gate
    from database import landscaper_list_books
    return JSONResponse({"books": await asyncio.to_thread(landscaper_list_books)})


async def _require_book(code: str):
    from database import landscaper_book_exists
    ok = await asyncio.to_thread(landscaper_book_exists, code)
    return None if ok else JSONResponse({"error": "unknown book"}, status_code=404)


@app.get("/api/landscaper/books/{code}")
async def api_landscaper_get_book(code: str):
    from database import landscaper_get_book
    data = await asyncio.to_thread(landscaper_get_book, code)
    if data is None:
        return JSONResponse({"error": "unknown book"}, status_code=404)
    return JSONResponse(data)


@app.post("/api/landscaper/books/{code}/clients")
async def api_landscaper_add_client(code: str, request: Request):
    bad = await _require_book(code)
    if bad:
        return bad
    body = await request.json()
    price = _coerce_float(body.get("price"))
    name = (body.get("name") or "").strip()
    zip_code = (body.get("zip") or "").strip()
    freq = (body.get("freq") or "w").strip()
    if not name or not zip_code or price is None:
        return JSONResponse({"error": "name, zip, numeric price required"},
                            status_code=400)
    from database import landscaper_add_client
    cid = await asyncio.to_thread(landscaper_add_client, code, name,
                                  zip_code, price, freq)
    return JSONResponse({"id": cid})


@app.delete("/api/landscaper/books/{code}/clients/{client_id}")
async def api_landscaper_delete_client(code: str, client_id: int):
    bad = await _require_book(code)
    if bad:
        return bad
    from database import landscaper_delete_client
    ok = await asyncio.to_thread(landscaper_delete_client, code, client_id)
    return JSONResponse({"ok": ok})


@app.post("/api/landscaper/books/{code}/costs")
async def api_landscaper_save_costs(code: str, request: Request):
    bad = await _require_book(code)
    if bad:
        return bad
    body = await request.json()
    costs = body.get("costs") if isinstance(body.get("costs"), dict) else {}
    # Coerce to plain numbers so a junk payload can't poison the store.
    clean = {k: v for k, v in costs.items() if isinstance(v, (int, float))}
    from database import landscaper_save_costs
    ok = await asyncio.to_thread(landscaper_save_costs, code, clean)
    return JSONResponse({"ok": ok})


# ── Household finance dashboard ─────────────────────────────────────
# Admin creates a book and texts the /household?book=CODE link; the code
# is the auth (same pattern as the landscaper book + prototype tokens).
# Descriptions are redacted BEFORE storage; raw CSVs are never persisted.

@app.get("/household")
async def household_page(request: Request, book: str = ""):
    """Bank-statement bucketing dashboard. ?book=<code> connects a shared
    server-side book; no code → the admin 'create a book' landing."""
    from database import household_book_exists
    book = (book or "").strip()
    valid = bool(book) and await asyncio.to_thread(household_book_exists, book)
    return templates.TemplateResponse("household.html", {
        "request": request, "book": book if valid else "",
    })


@app.get("/casa", include_in_schema=False)
async def casa_alias(request: Request, book: str = ""):
    """Short textable alias for the household dashboard."""
    dest = "/household" + (f"?book={book.strip()}" if book.strip() else "")
    return RedirectResponse(dest, status_code=302)


@app.post("/api/household/books")
async def api_hh_create_book(request: Request):
    """Admin: create a household book. Body: {name}. Returns {code}."""
    gate = _admin_gate(request)
    if gate:
        return gate
    body = await request.json()
    from database import household_create_book
    code = await asyncio.to_thread(household_create_book,
                                   (body.get("name") or "Household").strip())
    if not code:
        return JSONResponse({"error": "could not create book"}, status_code=500)
    return JSONResponse({"code": code})


@app.get("/api/household/books")
async def api_hh_list_books(request: Request):
    """Admin: list all household books."""
    gate = _admin_gate(request)
    if gate:
        return gate
    from database import household_list_books
    return JSONResponse({"books": await asyncio.to_thread(household_list_books)})


async def _require_hh_book(code: str):
    from database import household_book_exists
    ok = await asyncio.to_thread(household_book_exists, code)
    return None if ok else JSONResponse({"error": "unknown book"}, status_code=404)


@app.get("/api/household/books/{code}")
async def api_hh_get_book(code: str):
    """State the page needs: accounts, month list, the bucket catalog."""
    import household
    from database import household_list_accounts, household_all_txns
    bad = await _require_hh_book(code)
    if bad:
        return bad
    accounts = await asyncio.to_thread(household_list_accounts, code)
    txns = await asyncio.to_thread(household_all_txns, code)
    months = sorted({t["date"][:7] for t in txns if t["date"]})
    return JSONResponse({
        "accounts": accounts,
        "months": months,
        "txn_count": len(txns),
        "buckets": list(household.BUCKET_CLASS.keys()),
    })


@app.post("/api/household/books/{code}/accounts")
async def api_hh_add_account(code: str, request: Request):
    bad = await _require_hh_book(code)
    if bad:
        return bad
    body = await request.json()
    name = (body.get("name") or "").strip()
    kind = (body.get("kind") or "checking").strip()
    if not name:
        return JSONResponse({"error": "name required"}, status_code=400)
    from database import household_add_account
    aid = await asyncio.to_thread(household_add_account, code, name, kind,
                                  body.get("mapping") if isinstance(body.get("mapping"), dict) else None)
    return JSONResponse({"id": aid})


@app.delete("/api/household/books/{code}/accounts/{account_id}")
async def api_hh_delete_account(code: str, account_id: int):
    bad = await _require_hh_book(code)
    if bad:
        return bad
    from database import household_delete_account
    ok = await asyncio.to_thread(household_delete_account, code, account_id)
    return JSONResponse({"ok": ok})


@app.post("/api/household/books/{code}/preview")
async def api_hh_preview(code: str, request: Request):
    """Parse an uploaded CSV's headers + first rows and guess the column
    mapping, for the confirm-before-import step. Nothing is stored."""
    import household
    bad = await _require_hh_book(code)
    if bad:
        return bad
    body = await request.json()
    text = body.get("csv") or ""
    if not isinstance(text, str) or not text.strip():
        return JSONResponse({"error": "empty csv"}, status_code=400)
    headers, rows = household.parse_csv(text)
    if not headers:
        return JSONResponse({"error": "could not parse csv"}, status_code=400)
    mapping = household.auto_detect_mapping(headers, rows)
    return JSONResponse({
        "headers": headers,
        "sample": rows[:5],
        "row_count": len(rows),
        "mapping": mapping,
    })


@app.post("/api/household/books/{code}/import")
async def api_hh_import(code: str, request: Request):
    """Normalize + categorize + store a CSV into an account. Applies the
    book's learned rules; remembers the mapping for next time."""
    import household
    from database import (household_get_rules, household_insert_txns,
                          household_set_mapping, household_set_account_balance)
    bad = await _require_hh_book(code)
    if bad:
        return bad
    body = await request.json()
    text = body.get("csv") or ""
    mapping = body.get("mapping")
    account_id = body.get("account_id")
    if not isinstance(text, str) or not text.strip() or not isinstance(mapping, dict):
        return JSONResponse({"error": "csv and mapping required"}, status_code=400)
    try:
        account_id = int(account_id)
    except (TypeError, ValueError):
        return JSONResponse({"error": "valid account_id required"}, status_code=400)

    def _do():
        headers, rows = household.parse_csv(text)
        learned = household_get_rules(code)
        txns = household.normalize_rows(headers, rows, mapping, learned)
        inserted = household_insert_txns(code, account_id, txns)
        household_set_mapping(code, account_id, mapping)
        # Read the closing balance straight off the statement (HELOC /
        # savings balances feed the decision system with no manual entry).
        bal, bal_date = household.extract_last_balance(headers, rows, mapping)
        if bal is not None:
            household_set_account_balance(code, account_id, bal, bal_date)
        return len(txns), inserted

    parsed, inserted = await asyncio.to_thread(_do)
    return JSONResponse({"parsed": parsed, "inserted": inserted,
                         "skipped": parsed - inserted})


@app.post("/api/household/books/{code}/import-pdf")
async def api_hh_import_pdf(code: str, request: Request):
    """Import a Golden 1 PDF statement (no CSV needed). Parses every
    account in the statement, creates/matches each by name, stores the
    transactions, reads balances, and — for a HELOC — auto-fills the
    decision-system settings (balance / APR / payment) from the summary."""
    import base64, household, golden1_pdf
    from database import (household_list_accounts, household_add_account,
                          household_insert_txns, household_set_account_balance,
                          household_get_rules, household_get_settings,
                          household_set_settings)
    bad = await _require_hh_book(code)
    if bad:
        return bad
    body = await request.json()
    b64 = body.get("pdf") or ""
    if "," in b64[:64]:                      # strip a data: URL prefix
        b64 = b64.split(",", 1)[1]
    try:
        pdf = base64.b64decode(b64)
    except Exception:
        return JSONResponse({"error": "bad pdf data"}, status_code=400)
    if not pdf:
        return JSONResponse({"error": "empty pdf"}, status_code=400)

    def _do():
        try:
            parsed = golden1_pdf.parse(pdf)
        except RuntimeError as e:
            return {"error": str(e)}
        except Exception as e:
            logger.error(f"golden1 pdf parse failed: {e}")
            return {"error": "could not read that statement PDF"}
        learned = household_get_rules(code)
        existing = {a["name"]: a for a in household_list_accounts(code)}
        results, settings_update = [], {}
        for acct in parsed["accounts"]:
            txns = acct["transactions"]
            if learned:
                for t in txns:
                    if t["bucket"] == "Uncategorized":
                        nb = household.categorize(t["desc"], learned)
                        if nb != "Uncategorized":
                            t["bucket"] = nb
            aid = (existing[acct["name"]]["id"] if acct["name"] in existing
                   else household_add_account(code, acct["name"], acct["kind"], None))
            ins = household_insert_txns(code, aid, txns) if txns else 0
            bal = acct["summary"].get("balance")
            if bal is not None:
                # date the balance by the statement's latest activity, so an
                # older statement can't clobber a newer cash-on-hand figure
                bal_date = max((t["date"] for t in txns if t.get("date")), default=None)
                household_set_account_balance(code, aid, bal, bal_date)
            if acct["kind"] == "heloc":
                s = acct["summary"]
                if s.get("balance") is not None:
                    settings_update["heloc_balance"] = s["balance"]
                if s.get("apr") is not None:
                    settings_update["heloc_apr"] = s["apr"]
                if s.get("min_payment") is not None:
                    settings_update["heloc_payment"] = s["min_payment"]
                if s.get("credit_limit") is not None:
                    settings_update["heloc_limit"] = s["credit_limit"]
            elif acct["kind"] == "credit_card":
                s = acct["summary"]
                if s.get("balance") is not None:
                    settings_update["card_balance"] = s["balance"]
                if s.get("apr") is not None:
                    settings_update["card_apr"] = s["apr"]
                if s.get("min_payment") is not None:
                    settings_update["card_payment"] = s["min_payment"]
            results.append({"account": acct["name"], "kind": acct["kind"],
                            "parsed": len(txns), "inserted": ins, "balance": bal})
        if settings_update:
            cur = household_get_settings(code)
            cur.update(settings_update)
            household_set_settings(code, cur)
        # Auto-tag renovation vendors into any project (less manual work).
        from database import (household_list_projects, household_all_txns,
                              household_tag_txns)
        projects = household_list_projects(code)
        auto_tagged = 0
        if projects:
            from database import household_recategorize
            payees = (household_get_settings(code).get("reno_payees") or [])
            all_tx = household_all_txns(code)
            for r in all_tx:
                r.setdefault("project_id", r.get("project_id"))
            for p in projects:
                sugg = household.suggest_reno(all_tx, p.get("start"), p.get("end"), payees)
                ids = [t["id"] for t in sugg]
                if ids:
                    auto_tagged += household_tag_txns(code, ids, p["id"])
                    for t in sugg:
                        t["project_id"] = p["id"]        # keep local view in sync
                        # A learned labor payee's payment shouldn't linger in
                        # the review tray — book it as Home Improvement.
                        if household.is_p2p(t["desc"]) and t.get("bucket") == "Uncategorized":
                            household_recategorize(code, t["id"], "Home Improvement")
        return {"type": parsed["type"], "accounts": results,
                "settings_updated": bool(settings_update),
                "auto_tagged": auto_tagged}

    out = await asyncio.to_thread(_do)
    if isinstance(out, dict) and out.get("error"):
        return JSONResponse(out, status_code=400)
    return JSONResponse(out)


@app.get("/api/household/books/{code}/dashboard")
async def api_hh_dashboard(code: str, month: str = ""):
    """The whole dashboard payload: headline figures, spend-by-bucket,
    monthly trend, recurring bills, and the scoped transactions + review
    tray. Aggregates computed server-side over the redacted ledger."""
    import household
    from database import (household_all_txns, household_list_accounts,
                          household_list_projects)
    bad = await _require_hh_book(code)
    if bad:
        return bad
    rows = await asyncio.to_thread(household_all_txns, code)
    accounts = await asyncio.to_thread(household_list_accounts, code)
    projects = await asyncio.to_thread(household_list_projects, code)
    acct_name = {a["id"]: a["name"] for a in accounts}
    # Enrich stored rows into engine txns (class + merchant key).
    for r in rows:
        r["cls"] = household.bucket_class(r["bucket"])
        r["mkey"] = household.merchant_key(r["desc"])
    month = (month or "").strip() or None
    if month is None and rows:
        month = sorted({r["date"][:7] for r in rows if r["date"]})[-1]
    summary = household.summarize(rows, month)
    recurring = household.find_recurring(rows)
    scoped = [r for r in rows if (month is None or (r["date"] or "")[:7] == month)]
    scoped.sort(key=lambda r: (r["date"] or "", r["id"]), reverse=True)
    txns = [{"id": r["id"], "date": r["date"], "desc": r["desc"],
             "amount": round(r["amount"], 2), "bucket": r["bucket"],
             "cls": r["cls"], "account": acct_name.get(r["account_id"], ""),
             "project_id": r.get("project_id")}
            for r in scoped]
    review = [t for t in txns if t["cls"] == "review"]
    return JSONResponse({
        "summary": summary, "recurring": recurring[:20],
        "txns": txns, "review": review,
        "projects": projects,
        "buckets": list(household.BUCKET_CLASS.keys()),
    })


@app.post("/api/household/books/{code}/txns/{txn_id}/bucket")
async def api_hh_recategorize(code: str, txn_id: int, request: Request):
    """Assign a bucket to a transaction. With apply_all, learns a rule
    from the merchant and re-buckets every other Uncategorized match."""
    import household
    from database import household_recategorize, household_add_rule
    bad = await _require_hh_book(code)
    if bad:
        return bad
    body = await request.json()
    bucket = (body.get("bucket") or "").strip()
    if bucket not in household.BUCKET_CLASS:
        return JSONResponse({"error": "unknown bucket"}, status_code=400)
    like = None
    if body.get("apply_all"):
        like = household.merchant_key(body.get("desc") or "")
        like = like if len(like) >= 3 else None

    def _do():
        n = household_recategorize(code, txn_id, bucket, like)
        if like:
            household_add_rule(code, like, bucket)
        return n

    updated = await asyncio.to_thread(_do)
    return JSONResponse({"updated": updated})


# ── Household phase 2: decision system + renovation project ─────────
@app.get("/api/household/books/{code}/settings")
async def api_hh_get_settings(code: str):
    from database import household_get_settings, household_list_accounts
    bad = await _require_hh_book(code)
    if bad:
        return bad
    settings = await asyncio.to_thread(household_get_settings, code)
    accounts = await asyncio.to_thread(household_list_accounts, code)
    return JSONResponse({"settings": settings, "accounts": accounts})


@app.post("/api/household/books/{code}/settings")
async def api_hh_set_settings(code: str, request: Request):
    """Merge the provided target fields into the book's settings (so
    saving the reno budget doesn't clear the cushion goal, etc.)."""
    from database import household_get_settings, household_set_settings
    bad = await _require_hh_book(code)
    if bad:
        return bad
    body = await request.json()
    incoming = body.get("settings") if isinstance(body.get("settings"), dict) else {}
    allowed = {"mode", "income", "savings", "savings_extra", "cushion_goal",
               "heloc_balance", "heloc_apr", "heloc_payment", "heloc_limit",
               "card_balance", "card_apr", "card_payment", "reno_budget",
               "home_value"}
    clean = {}
    for k, v in incoming.items():
        if k not in allowed:
            continue
        if k == "mode":
            clean[k] = str(v)[:20]
        else:
            fv = _coerce_float(v)
            if fv is not None:
                clean[k] = fv

    def _do():
        cur = household_get_settings(code)
        cur.update(clean)
        household_set_settings(code, cur)
        return cur

    saved = await asyncio.to_thread(_do)
    return JSONResponse({"settings": saved})


@app.get("/api/household/books/{code}/roadmap")
async def api_hh_roadmap(code: str):
    """The framework: one ordered path over cash flow, debts, the kitchen
    goal and retirement — with the single next move she should focus on."""
    import household
    from database import (household_all_txns, household_get_settings,
                          household_list_projects, household_budget_items,
                          household_list_accounts)
    bad = await _require_hh_book(code)
    if bad:
        return bad

    def _do():
        rows = household_all_txns(code)
        for r in rows:
            r["cls"] = household.bucket_class(r["bucket"])
        settings = household_get_settings(code)
        accounts = household_list_accounts(code)
        vitals = household.vital_signs(rows, settings, accounts)

        # Reno summary: total planned across projects with a budget, and how
        # much of it the HELOC could cover (same headroom math as the budget).
        reno = {"active": False, "budget_total": 0.0, "can_fund": 0.0}
        for p in household_list_projects(code):
            items = household_budget_items(code, p["id"])
            if not items:
                continue
            meta = _budget_meta_with_financing(code, p["id"])
            summ = household.budget_summary(items, meta)
            reno["active"] = True
            reno["budget_total"] += summ["total"]
            reno["can_fund"] += summ["financing"]["can_fund"]
        reno["budget_total"] = round(reno["budget_total"], 2)
        reno["can_fund"] = round(reno["can_fund"], 2)

        # Retirement summary from the same plan the Retirement tab shows.
        plan = household.retirement_plan(settings)
        retire = {"configured": False}
        if plan.get("configured") and plan.get("chosen"):
            ch = plan["chosen"]
            retire = {"configured": True, "year": ch["year"],
                      "covered": ch["covered"], "surplus": ch["surplus_with_ss"]}

        return household.money_roadmap(vitals, reno=reno, retire=retire)

    return JSONResponse(await asyncio.to_thread(_do))


@app.get("/api/household/books/{code}/net-worth")
async def api_hh_net_worth(code: str):
    """Assets − liabilities: home + cash + investments vs. the mortgage,
    HELOC and card the tool tracks."""
    import household
    from database import household_get_settings, household_list_accounts
    bad = await _require_hh_book(code)
    if bad:
        return bad

    def _do():
        settings = household_get_settings(code)
        accounts = household_list_accounts(code)
        return household.net_worth(settings, accounts)
    return JSONResponse(await asyncio.to_thread(_do))


@app.post("/api/household/books/{code}/assets")
async def api_hh_asset_save(code: str, request: Request):
    """Add or update a manual asset (Fidelity, 401k, a second property…).
    Body: {id?, name, value, kind}. Returns the updated net-worth picture."""
    import household
    from database import household_get_settings, household_set_settings, household_list_accounts
    bad = await _require_hh_book(code)
    if bad:
        return bad
    body = await request.json()
    name = str(body.get("name") or "").strip()
    value = _coerce_float(body.get("value"))
    if not name or value is None:
        return JSONResponse({"error": "name and a numeric value are required"}, status_code=400)
    kind = str(body.get("kind") or "investment")
    if kind not in ("investment", "property", "cash", "other"):
        kind = "investment"

    def _do():
        settings = household_get_settings(code)
        assets = [a for a in (settings.get("assets") or []) if isinstance(a, dict)]
        aid = body.get("id")
        row = next((a for a in assets if a.get("id") == aid), None) if aid is not None else None
        if row:
            row.update(name=name[:60], value=value, kind=kind)
        else:
            new_id = (max([a.get("id", 0) for a in assets], default=0) or 0) + 1
            assets.append({"id": new_id, "name": name[:60], "value": value, "kind": kind})
        settings["assets"] = assets
        household_set_settings(code, settings)
        return household.net_worth(settings, household_list_accounts(code))
    return JSONResponse(await asyncio.to_thread(_do))


@app.delete("/api/household/books/{code}/assets/{asset_id}")
async def api_hh_asset_delete(code: str, asset_id: int):
    import household
    from database import household_get_settings, household_set_settings, household_list_accounts
    bad = await _require_hh_book(code)
    if bad:
        return bad

    def _do():
        settings = household_get_settings(code)
        settings["assets"] = [a for a in (settings.get("assets") or [])
                              if isinstance(a, dict) and a.get("id") != asset_id]
        household_set_settings(code, settings)
        return household.net_worth(settings, household_list_accounts(code))
    return JSONResponse(await asyncio.to_thread(_do))


@app.get("/api/household/books/{code}/retirement")
async def api_hh_retirement(code: str):
    """The retirement picture: pension + Social Security vs. living costs and
    the debts the tool already tracks, across candidate retirement years."""
    import household
    from database import household_get_settings
    bad = await _require_hh_book(code)
    if bad:
        return bad
    settings = await asyncio.to_thread(household_get_settings, code)
    return JSONResponse(household.retirement_plan(settings))


@app.post("/api/household/books/{code}/retirement/seed")
async def api_hh_retirement_seed(code: str):
    """Populate the retirement plan with Frances's CalPERS/SS/mortgage
    starting figures (from her spreadsheet). Editable afterward."""
    import household
    from database import household_get_settings, household_set_settings
    bad = await _require_hh_book(code)
    if bad:
        return bad

    def _do():
        cur = household_get_settings(code)
        if not (cur.get("retirement") or {}).get("pension_by_year"):
            cur["retirement"] = household.retirement_seed()
            household_set_settings(code, cur)
        return household.retirement_plan(cur)
    return JSONResponse(await asyncio.to_thread(_do))


@app.post("/api/household/books/{code}/retirement")
async def api_hh_retirement_save(code: str, request: Request):
    """Merge edits into settings['retirement'] — the picked retire year / SS
    claim age, monthly expenses, mortgage, or a whole pension/SS schedule."""
    import household
    from database import household_get_settings, household_set_settings
    bad = await _require_hh_book(code)
    if bad:
        return bad
    body = await request.json()
    incoming = body.get("retirement") if isinstance(body.get("retirement"), dict) else {}
    num_fields = {"birth_year", "retire_expenses", "mortgage_balance",
                  "mortgage_payment", "mortgage_rate", "retire_year", "ss_claim_age"}
    clean = {}
    for k, v in incoming.items():
        if k in num_fields:
            fv = _coerce_float(v)
            if fv is not None:
                clean[k] = int(fv) if k in ("birth_year", "retire_year", "ss_claim_age") else fv
        elif k in ("pension_by_year", "ss_by_age") and isinstance(v, dict):
            sched = {}
            for yk, yv in v.items():
                fv = _coerce_float(yv)
                if fv is not None and str(yk).strip().isdigit():
                    sched[str(int(yk))] = fv
            if sched:
                clean[k] = sched

    def _do():
        cur = household_get_settings(code)
        ret = cur.get("retirement") or {}
        if not ret.get("pension_by_year"):
            ret = household.retirement_seed()
        ret.update(clean)
        cur["retirement"] = ret
        household_set_settings(code, cur)
        return household.retirement_plan(cur)
    return JSONResponse(await asyncio.to_thread(_do))


@app.get("/api/household/books/{code}/this-month")
async def api_hh_this_month(code: str, mode: str = "kill_debt"):
    """The decision tab: four vital signs + the chosen mode's one move."""
    import household
    from database import (household_all_txns, household_get_settings,
                          household_list_accounts)
    bad = await _require_hh_book(code)
    if bad:
        return bad
    rows = await asyncio.to_thread(household_all_txns, code)
    settings = await asyncio.to_thread(household_get_settings, code)
    accounts = await asyncio.to_thread(household_list_accounts, code)
    for r in rows:
        r["cls"] = household.bucket_class(r["bucket"])
    return JSONResponse(household.this_month(rows, settings, mode, accounts))


@app.post("/api/household/books/{code}/projects")
async def api_hh_create_project(code: str, request: Request):
    from database import household_create_project
    bad = await _require_hh_book(code)
    if bad:
        return bad
    body = await request.json()
    name = (body.get("name") or "").strip()
    if not name:
        return JSONResponse({"error": "name required"}, status_code=400)
    pid = await asyncio.to_thread(
        household_create_project, code, name,
        (body.get("start") or "").strip() or None,
        (body.get("end") or "").strip() or None,
        _coerce_float(body.get("budget")))
    return JSONResponse({"id": pid})


@app.get("/api/household/books/{code}/projects")
async def api_hh_list_projects(code: str):
    from database import household_list_projects
    bad = await _require_hh_book(code)
    if bad:
        return bad
    return JSONResponse({"projects": await asyncio.to_thread(household_list_projects, code)})


@app.delete("/api/household/books/{code}/projects/{pid}")
async def api_hh_delete_project(code: str, pid: int):
    from database import household_delete_project
    bad = await _require_hh_book(code)
    if bad:
        return bad
    ok = await asyncio.to_thread(household_delete_project, code, pid)
    return JSONResponse({"ok": ok})


@app.post("/api/household/books/{code}/projects/{pid}/tag")
async def api_hh_tag(code: str, pid: int, request: Request):
    """Tag (or untag, with tag=false) transactions to a renovation project."""
    from database import household_tag_txns
    bad = await _require_hh_book(code)
    if bad:
        return bad
    body = await request.json()
    ids = body.get("txn_ids") or []
    if not isinstance(ids, list) or not ids:
        return JSONResponse({"error": "txn_ids required"}, status_code=400)
    target = pid if body.get("tag", True) else None
    n = await asyncio.to_thread(household_tag_txns, code, ids, target)
    return JSONResponse({"tagged": n})


@app.post("/api/household/books/{code}/projects/{pid}/tag-labor")
async def api_hh_tag_labor(code: str, pid: int, request: Request):
    """Mark a person-to-person payment (a Zelle to a contractor) as
    renovation labor and REMEMBER the payee: the payee is learned, every
    matching payment in the project window is tagged to the project and
    re-bucketed to Home Improvement, and future imports auto-tag it."""
    import household
    from database import (household_all_txns, household_get_settings,
                          household_set_settings, household_tag_txns,
                          household_recategorize, household_list_projects)
    bad = await _require_hh_book(code)
    if bad:
        return bad
    body = await request.json()
    try:
        txn_id = int(body.get("txn_id"))
    except (TypeError, ValueError):
        return JSONResponse({"error": "a numeric txn_id is required"}, status_code=400)

    def _do():
        rows = household_all_txns(code)
        row = next((r for r in rows if r["id"] == txn_id), None)
        if not row:
            return {"error": "unknown transaction"}
        payee = household.payee_fragment(row["desc"])
        if not payee:
            return {"error": "could not read a payee name"}
        settings = household_get_settings(code)
        payees = [p for p in (settings.get("reno_payees") or []) if p]
        if payee not in payees:
            payees.append(payee)
            settings["reno_payees"] = payees
            household_set_settings(code, settings)
        proj = next((p for p in household_list_projects(code) if p["id"] == pid), None)
        start = proj.get("start") if proj else None
        end = proj.get("end") if proj else None

        def in_window(r):
            d = r.get("date", "") or ""
            return not ((start and d < start) or (end and d > end))
        # tag + re-bucket every matching payment in the window that isn't
        # already tagged to a DIFFERENT project (whole-word payee match)
        matches = [r for r in rows
                   if household.payee_matches(r["desc"], payee) and r["amount"] < 0
                   and (not r.get("project_id") or r.get("project_id") == pid)
                   and in_window(r)]
        ids = [r["id"] for r in matches]
        tagged = household_tag_txns(code, ids, pid) if ids else 0
        for r in matches:
            household_recategorize(code, r["id"], "Home Improvement")
        return {"payee": payee, "tagged": tagged, "learned": True}

    out = await asyncio.to_thread(_do)
    if out.get("error"):
        return JSONResponse(out, status_code=400)
    return JSONResponse(out)


@app.post("/api/household/books/{code}/reno-payees")
async def api_hh_reno_payees(code: str, request: Request):
    """Remove a learned renovation-labor payee (add happens via tag-labor)."""
    from database import household_get_settings, household_set_settings
    bad = await _require_hh_book(code)
    if bad:
        return bad
    body = await request.json()
    remove = str(body.get("remove") or "").strip().lower()
    if not remove:
        return JSONResponse({"error": "remove (a payee name) is required"}, status_code=400)

    def _do():
        settings = household_get_settings(code)
        payees = [p for p in (settings.get("reno_payees") or []) if p and p != remove]
        settings["reno_payees"] = payees
        household_set_settings(code, settings)
        return {"reno_payees": payees}
    return JSONResponse(await asyncio.to_thread(_do))


@app.get("/api/household/books/{code}/projects/{pid}")
async def api_hh_project(code: str, pid: int):
    """The renovation ledger: reconciliation, vendor breakdown, true cost,
    payoff inputs, the tagged rows, and reno-vendor suggestions to tag."""
    import household
    from database import (household_all_txns, household_get_settings,
                          household_list_projects, household_list_accounts,
                          household_interest_paid)
    bad = await _require_hh_book(code)
    if bad:
        return bad
    projects = await asyncio.to_thread(household_list_projects, code)
    proj = next((p for p in projects if p["id"] == pid), None)
    if not proj:
        return JSONResponse({"error": "unknown project"}, status_code=404)
    rows = await asyncio.to_thread(household_all_txns, code)
    settings = await asyncio.to_thread(household_get_settings, code)
    accounts = await asyncio.to_thread(household_list_accounts, code)
    interest_paid = await asyncio.to_thread(household_interest_paid, code)
    acct_name = {a["id"]: a["name"] for a in accounts}
    for r in rows:
        r["cls"] = household.bucket_class(r["bucket"])
    payees = settings.get("reno_payees") or []
    tagged = [r for r in rows if r.get("project_id") == pid]
    summary = household.project_summary(
        tagged, {**settings, "reno_budget": proj.get("budget") or 0}, interest_paid)
    suggestions = household.suggest_reno(rows, proj.get("start"), proj.get("end"), payees)
    candidates = household.labor_candidates(rows, proj.get("start"), proj.get("end"), payees)

    def fmt(r):
        return {"id": r["id"], "date": r["date"], "desc": r["desc"],
                "amount": round(r["amount"], 2), "bucket": r["bucket"],
                "account": acct_name.get(r["account_id"], ""),
                "payee": r.get("payee")}
    tagged.sort(key=lambda r: (r["date"] or ""), reverse=True)
    return JSONResponse({
        "project": proj, "summary": summary,
        "tagged": [fmt(r) for r in tagged],
        "suggestions": [fmt(r) for r in suggestions],
        "labor_candidates": [fmt(r) for r in candidates],
        "reno_payees": payees,
    })


# ── Kitchen budget builder ─────────────────────────────────────────
def _budget_meta_with_financing(code, pid):
    """Project meta + live HELOC balance/limit pulled from settings, so the
    headroom math has the real numbers without re-entering them."""
    from database import household_project_meta, household_get_settings
    meta = household_project_meta(code, pid)
    s = household_get_settings(code)
    if s.get("heloc_balance") is not None:
        meta.setdefault("heloc_balance", s["heloc_balance"])
    if s.get("heloc_limit") is not None:
        meta.setdefault("heloc_limit", s["heloc_limit"])
    meta.setdefault("home_value", 950000)   # she told us; editable
    return meta


@app.get("/api/household/books/{code}/projects/{pid}/budget")
async def api_hh_budget(code: str, pid: int):
    import household
    from database import household_budget_items
    bad = await _require_hh_book(code)
    if bad:
        return bad

    def _do():
        items = household_budget_items(code, pid)
        meta = _budget_meta_with_financing(code, pid)
        return {"items": items, "meta": meta,
                "summary": household.budget_summary(items, meta),
                "sections": household.BUDGET_SECTIONS}
    return JSONResponse(await asyncio.to_thread(_do))


@app.post("/api/household/books/{code}/projects/{pid}/budget/seed")
async def api_hh_budget_seed(code: str, pid: int):
    """Fill an empty budget with the researched San Leandro kitchen template
    (scaled to any measurements already saved)."""
    import household
    from database import (household_budget_items, household_budget_bulk_add,
                          household_project_meta)
    bad = await _require_hh_book(code)
    if bad:
        return bad

    def _do():
        if household_budget_items(code, pid):
            return {"seeded": 0, "note": "already has items"}
        meta = household_project_meta(code, pid)
        items = household.kitchen_seed_template(meta)
        return {"seeded": household_budget_bulk_add(code, pid, items)}
    return JSONResponse(await asyncio.to_thread(_do))


@app.post("/api/household/books/{code}/projects/{pid}/budget/items")
async def api_hh_budget_add(code: str, pid: int, request: Request):
    from database import household_budget_add
    bad = await _require_hh_book(code)
    if bad:
        return bad
    item = await request.json()
    iid = await asyncio.to_thread(household_budget_add, code, pid, item)
    return JSONResponse({"id": iid})


@app.post("/api/household/books/{code}/projects/{pid}/budget/items/{item_id}")
async def api_hh_budget_update(code: str, pid: int, item_id: int, request: Request):
    from database import household_budget_update
    bad = await _require_hh_book(code)
    if bad:
        return bad
    fields = await request.json()
    ok = await asyncio.to_thread(household_budget_update, code, item_id, fields)
    return JSONResponse({"ok": ok})


@app.post("/api/household/books/{code}/projects/{pid}/budget/items/{item_id}/choose")
async def api_hh_budget_choose(code: str, pid: int, item_id: int):
    """Pick this alternative in its option group (unpicks the siblings)."""
    from database import household_budget_choose
    bad = await _require_hh_book(code)
    if bad:
        return bad
    ok = await asyncio.to_thread(household_budget_choose, code, item_id)
    return JSONResponse({"ok": ok})


@app.delete("/api/household/books/{code}/projects/{pid}/budget/items/{item_id}")
async def api_hh_budget_delete(code: str, pid: int, item_id: int):
    from database import household_budget_delete
    bad = await _require_hh_book(code)
    if bad:
        return bad
    ok = await asyncio.to_thread(household_budget_delete, code, item_id)
    return JSONResponse({"ok": ok})


@app.post("/api/household/books/{code}/projects/{pid}/meta")
async def api_hh_project_meta(code: str, pid: int, request: Request):
    """Save measurements / contingency% / home value / mortgage / target CLTV
    / budget target onto the project."""
    from database import household_project_set_meta
    bad = await _require_hh_book(code)
    if bad:
        return bad
    body = await request.json()
    incoming = body.get("meta") if isinstance(body.get("meta"), dict) else {}
    allowed = {"floor_len_ft", "floor_wid_ft", "counter_run_ft", "counter_depth_in",
               "cabinet_lf", "backsplash_len_ft", "backsplash_height_in", "ceiling_ht_ft",
               "floor_sqft", "counter_sqft", "backsplash_sqft",
               "contingency_pct", "budget_target", "home_value",
               "mortgage_balance", "target_cltv", "notes"}
    clean = {}
    for k, v in incoming.items():
        if k not in allowed:
            continue
        if k == "notes":
            clean[k] = str(v)[:400]
        else:
            fv = _coerce_float(v)
            if fv is not None:
                clean[k] = fv
    ok = await asyncio.to_thread(household_project_set_meta, code, pid, clean)
    return JSONResponse({"ok": ok})


@app.post("/api/household/books/{code}/projects/{pid}/budget/fetch-url")
async def api_hh_budget_fetch(code: str, pid: int, request: Request):
    """Read a product page she pastes/drops and pull the item name + price
    (Open Graph / JSON-LD / meta / a $-price fallback). Server-side fetch
    with an SSRF guard — no private hosts."""
    bad = await _require_hh_book(code)
    if bad:
        return bad
    body = await request.json()
    url = (body.get("url") or "").strip()
    info = await asyncio.to_thread(_fetch_product, url)
    if info.get("error"):
        return JSONResponse(info, status_code=400)
    return JSONResponse(info)


def _fetch_product(url: str) -> dict:
    import ipaddress
    import re as _re
    import socket
    import urllib.request
    from urllib.parse import urlparse
    if not url.lower().startswith(("http://", "https://")):
        return {"error": "paste a full http(s) link"}
    host = urlparse(url).hostname or ""
    try:                                        # SSRF guard: no private hosts
        for fam, *_rest in socket.getaddrinfo(host, None):
            ip = ipaddress.ip_address(_rest[-1][0])
            if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved:
                return {"error": "that host isn't allowed"}
    except Exception:
        return {"error": "could not resolve that link"}
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (compatible; MarketPulse/1.0)"})
        with urllib.request.urlopen(req, timeout=12) as r:
            html = r.read(600_000).decode("utf-8", "ignore")
    except Exception:
        return {"error": "could not open that link"}

    def meta(prop):
        m = _re.search(r'<meta[^>]+(?:property|name|itemprop)=["\']%s["\'][^>]+content=["\']([^"\']+)' % prop, html, _re.I) \
            or _re.search(r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+(?:property|name|itemprop)=["\']%s["\']' % prop, html, _re.I)
        return m.group(1).strip() if m else None

    name = meta("og:title") or meta("twitter:title")
    if not name:
        t = _re.search(r"<title[^>]*>([^<]+)</title>", html, _re.I)
        name = t.group(1).strip() if t else None
    price = meta("product:price:amount") or meta("og:price:amount") or meta("price")
    if not price:                               # JSON-LD "price": "12.34"
        m = _re.search(r'"price"\s*:\s*"?([0-9][0-9,]*\.?[0-9]*)"?', html)
        price = m.group(1) if m else None
    if not price:                               # last resort: first $ amount
        m = _re.search(r'\$\s?([0-9][0-9,]{1,7}(?:\.[0-9]{2})?)', html)
        price = m.group(1) if m else None
    price_val = None
    if price:
        try:
            price_val = float(str(price).replace(",", ""))
        except ValueError:
            price_val = None
    return {"name": (name or "")[:120], "price": price_val, "url": url}


# Browsers and iOS probe these absolute paths regardless of <link> tags —
# serve them so the access log stops filling with 404s.
_STATIC_DIR = Path(__file__).resolve().parent / "static"


@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    from fastapi.responses import FileResponse
    return FileResponse(_STATIC_DIR / "favicon.svg", media_type="image/svg+xml")


@app.get("/apple-touch-icon.png", include_in_schema=False)
@app.get("/apple-touch-icon-precomposed.png", include_in_schema=False)
async def apple_touch_icon():
    from fastapi.responses import FileResponse
    return FileResponse(_STATIC_DIR / "apple-touch-icon.png", media_type="image/png")


@app.get("/compounders")
async def compounders_page(request: Request):
    """The 14%/yr long-term screen: quality gates (ROIC, consistency,
    cash conversion, debt, capex) + a transparent expected-return
    decomposition (growth + buybacks + dividends ± valuation drift).
    Universe self-discovers from SEC EDGAR (≥ $1B revenue, incl. ADRs);
    data built monthly by refresh_compounders.yml."""
    from compounders import score, summary, data_source_label, ROIC_MIN, TARGET
    rows = score()
    return templates.TemplateResponse("compounders.html", {
        "request":          request,
        "rows":             rows,
        "compounder_cards": [r for r in rows if r["status"] == "COMPOUNDER"][:12],
        "summary":          summary(rows),
        "data_source":      data_source_label(),
        "roic_min":         ROIC_MIN,
        "target":           TARGET,
    })


@app.get("/aristocrats")
async def aristocrats_page(request: Request):
    """Dividend-aristocrat value screen: 25+ year raisers (US champions
    + Schwab-buyable international) flagged cheap when their yield sits
    ≥20% above their own 5-yr median (Geraldine Weiss), gated by the
    Chowder rule (yield + div growth ≥ 12; ≥ 8 utilities/REITs) and
    payout/leverage safety checks. Universe in aristocrats.py; live
    metrics via the monthly refresh-aristocrats workflow."""
    from aristocrats import (score, buy_list, data_source_label,
                             UNIVERSE, VALUE_PREMIUM_MIN,
                             CHOWDER_HURDLE, CHOWDER_HURDLE_LOW)
    rows = score()
    n_awaiting = sum(1 for r in rows if r["status"] == "AWAITING")
    return templates.TemplateResponse("aristocrats.html", {
        "request":       request,
        "rows":          rows,
        "buys":          buy_list(rows),
        "data_source":   data_source_label(),
        "total":         len(UNIVERSE),
        "n_awaiting":    n_awaiting,
        "premium_min":   VALUE_PREMIUM_MIN,
        "hurdle":        CHOWDER_HURDLE,
        "hurdle_low":    CHOWDER_HURDLE_LOW,
    })


@app.get("/global-values")
async def global_values(request: Request):
    """Country-level valuation vs OECD business cycle. Renders a
    ranked buy list + 2x2 quadrant chart + full sortable table.

    Snapshot data lives in country_data.py — hand-refreshed quarterly
    from Damodaran / OECD."""
    from country_data import (composite_scores, buy_list, LAST_UPDATED,
                              COUNTRIES, _cli_source_label)
    scored = composite_scores()
    return templates.TemplateResponse("global_values.html", {
        "request":         request,
        "countries":       scored,
        "buy_list":        buy_list(scored),
        "last_updated":    LAST_UPDATED,
        "cli_source":      _cli_source_label(),
        "total_countries": len(COUNTRIES),
    })


# ─── Pipeline CRM (admin-only) ──────────────────────────────────────
# Private two-person sales tracker — see BUILD_SPEC.md. Gated by the
# same ADMIN_TOKEN cookie used for /results. All write endpoints
# below also check.
@app.get("/pipeline")
async def pipeline(request: Request, funnel_start: str = "", funnel_end: str = ""):
    if not _check_pipeline_access(request):
        return RedirectResponse("/sign-in?redirect=/pipeline", status_code=303)
    from datetime import date as _date, datetime as _dt, timedelta as _td
    from crm import (STAGES, METRICS, STAGE_LABELS, METRIC_LABELS,
                     INDUSTRIES, EMAIL_TRIGGERS, ROLES,
                     HOSTING_MODELS, HOSTING_MODEL_LABELS,
                     list_contacts, arr_rollup, weekly_kpis, followup_due,
                     get_weekly_goals, iso_week_range,
                     funnel_conversion, trailing_weekly_kpis,
                     goals_completion_stats, arr_path_to_goal)

    def _parse_date(s: str, default: _date) -> _date:
        try:
            return _dt.strptime(s.strip(), "%Y-%m-%d").date() if s else default
        except ValueError:
            return default

    today = _date.today()
    f_end = _parse_date(funnel_end, today)
    f_start = _parse_date(funnel_start, today - _td(days=90))
    if f_start > f_end:
        f_start, f_end = f_end, f_start

    contacts = list_contacts()
    contacts_by_stage = {s: [c for c in contacts if c["stage"] == s] for s in STAGES}

    # JSON-safe copy for the detail-modal JS lookup (no Python date /
    # datetime objects survive tojson otherwise).
    def _js_safe(c: dict) -> dict:
        out = dict(c)
        for k in ("date_emailed", "next_date", "follow_up_date",
                  "created_at", "updated_at"):
            v = out.get(k)
            if v is not None and hasattr(v, "isoformat"):
                out[k] = v.isoformat()[:10]   # YYYY-MM-DD is enough for the UI
        return out
    contacts_json = [_js_safe(c) for c in contacts]
    week_start, week_end = iso_week_range()
    return templates.TemplateResponse("pipeline.html", {
        "request": request,
        "stages": STAGES,
        "stage_labels": STAGE_LABELS,
        "metrics": METRICS,
        "metric_labels": METRIC_LABELS,
        "contacts": contacts,
        "contacts_json": contacts_json,
        "contacts_by_stage": contacts_by_stage,
        "arr": arr_rollup(contacts),
        "followup_due": followup_due(contacts),
        "today": today,
        "kpis": weekly_kpis(),
        "goals": get_weekly_goals(),
        "week_start": week_start,
        "week_end": week_end,
        "funnel": funnel_conversion(f_start, f_end),
        "funnel_start": f_start,
        "funnel_end": f_end,
        "trailing": trailing_weekly_kpis(weeks=8),
        "completion": goals_completion_stats(),
        "path": arr_path_to_goal(contacts),
        "industries": INDUSTRIES,
        "email_triggers": EMAIL_TRIGGERS,
        "roles": ROLES,
        "hosting_models": HOSTING_MODELS,
        "hosting_model_labels": HOSTING_MODEL_LABELS,
    })


@app.post("/pipeline/contact")
async def pipeline_add_contact(request: Request):
    if not _check_pipeline_access(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    from crm import (add_contact, STAGES, INDUSTRIES, ROLES,
                     find_contact_by_email)
    form = await request.form()
    # Duplicate-email guard. If the supplied email already exists,
    # bounce back to /pipeline with a flag the page can surface.
    incoming_email = (form.get("email") or "").strip()
    if incoming_email:
        existing = find_contact_by_email(incoming_email)
        if existing:
            return RedirectResponse(
                f"/pipeline?dup_email={incoming_email}&existing_id={existing['id']}",
                status_code=303,
            )
    def _date(s: str | None):
        s = (s or "").strip()
        if not s:
            return None
        try:
            from datetime import datetime
            return datetime.strptime(s, "%Y-%m-%d").date()
        except ValueError:
            return None
    def _int(s: str | None) -> int:
        try:
            return int((s or "").strip() or 0)
        except ValueError:
            return 0
    stage = (form.get("stage") or "QUEUED").strip()
    if stage not in STAGES:
        stage = "QUEUED"
    industry = (form.get("industry") or "").strip() or None
    if industry and industry not in INDUSTRIES:
        industry = None
    role = (form.get("role") or "").strip() or None
    if role and role not in ROLES:
        role = None
    add_contact(
        name=(form.get("name") or "").strip(),
        title=(form.get("title") or "").strip() or None,
        agency=(form.get("agency") or "").strip() or None,
        email=(form.get("email") or "").strip() or None,
        stage=stage,
        pilot_value=_int(form.get("pilot_value")),
        recurring_value=_int(form.get("recurring_value")),
        date_emailed=_date(form.get("date_emailed")),
        next_date=_date(form.get("next_date")),
        subject=(form.get("subject") or "").strip() or None,
        notes=(form.get("notes") or "").strip() or None,
        industry=industry,
        role=role,
    )
    return RedirectResponse("/pipeline", status_code=303)


@app.post("/pipeline/stage")
async def pipeline_change_stage(request: Request):
    if not _check_pipeline_access(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    from crm import change_stage
    form = await request.form()
    try:
        contact_id = int(form.get("contact_id", "0"))
    except ValueError:
        contact_id = 0
    new_stage = (form.get("stage") or "").strip()
    if contact_id and new_stage:
        change_stage(contact_id, new_stage)
    return RedirectResponse("/pipeline", status_code=303)


@app.post("/pipeline/delete")
async def pipeline_delete_contact(request: Request):
    if not _check_pipeline_access(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    from crm import delete_contact
    form = await request.form()
    try:
        contact_id = int(form.get("contact_id", "0"))
    except ValueError:
        contact_id = 0
    if contact_id:
        delete_contact(contact_id)
    return RedirectResponse("/pipeline", status_code=303)


@app.post("/pipeline/goal")
async def pipeline_set_goal(request: Request):
    if not _check_pipeline_access(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    from crm import set_weekly_goal, METRICS
    form = await request.form()
    metric = (form.get("metric") or "").strip()
    try:
        target = int(form.get("target", "0"))
    except ValueError:
        target = 0
    if metric in METRICS and target >= 0:
        set_weekly_goal(metric, target)
    return RedirectResponse("/pipeline", status_code=303)


@app.post("/pipeline/update")
async def pipeline_update_contact(request: Request):
    """Patch a contact from the detail modal. Accepts any subset of
    editable fields and leaves the rest alone."""
    if not _check_pipeline_access(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    from crm import update_contact
    from datetime import datetime as _dt
    form = await request.form()
    try:
        cid = int(form.get("contact_id", "0"))
    except ValueError:
        cid = 0
    if not cid:
        return JSONResponse({"error": "missing contact_id"}, status_code=400)

    def _date(s: str | None):
        s = (s or "").strip()
        if not s:
            return None
        try:
            return _dt.strptime(s, "%Y-%m-%d").date()
        except ValueError:
            return None

    def _int_or_zero(s: str | None) -> int:
        try:
            return int((s or "").strip() or 0)
        except ValueError:
            return 0

    from crm import ROLES, HOSTING_MODELS
    role_raw = (form.get("role") or "").strip()
    role_val = role_raw if (role_raw == "" or role_raw in ROLES) else ""
    host_raw = (form.get("hosting_model") or "").strip()
    host_val = host_raw if host_raw in HOSTING_MODELS else "TBD"
    update_contact(
        cid,
        name=(form.get("name") or "").strip() or None,
        title=(form.get("title") or "").strip(),
        agency=(form.get("agency") or "").strip(),
        email=(form.get("email") or "").strip(),
        pilot_value=_int_or_zero(form.get("pilot_value")),
        recurring_value=_int_or_zero(form.get("recurring_value")),
        date_emailed=_date(form.get("date_emailed")),
        next_date=_date(form.get("next_date")),
        subject=(form.get("subject") or "").strip(),
        notes=(form.get("notes") or "").strip(),
        email_thread=(form.get("email_thread") or "").strip(),
        role=role_val,
        hosting_model=host_val,
        engagement_notes=(form.get("engagement_notes") or "").strip(),
        follow_up_date=_date(form.get("follow_up_date")),
    )
    return JSONResponse({"ok": True})


@app.post("/pipeline/industry")
async def pipeline_set_industry(request: Request):
    if not _check_pipeline_access(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    from crm import set_contact_industry, INDUSTRIES
    form = await request.form()
    try:
        contact_id = int(form.get("contact_id", "0"))
    except ValueError:
        contact_id = 0
    industry = (form.get("industry") or "").strip() or None
    if industry and industry not in INDUSTRIES:
        industry = None
    if contact_id:
        set_contact_industry(contact_id, industry)
    return RedirectResponse("/pipeline", status_code=303)


@app.get("/api/pipeline/find-by-email")
async def api_pipeline_find_by_email(request: Request, email: str = ""):
    """Returns {exists: bool, contact?: {...}} for the Add Contact
    form's preflight duplicate check."""
    if not _check_pipeline_access(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    from crm import find_contact_by_email
    hit = find_contact_by_email(email)
    return JSONResponse({"exists": bool(hit), "contact": hit})


@app.get("/api/pipeline/email/{contact_id}")
async def api_pipeline_email(request: Request, contact_id: int):
    """Render the suggested next-step email for a contact. Returns
    JSON the modal can drop into its textareas."""
    if not _check_pipeline_access(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    from crm import get_contact, suggest_email_for_contact
    contact = get_contact(contact_id)
    if not contact:
        return JSONResponse({"error": "not found"}, status_code=404)
    payload = suggest_email_for_contact(contact)
    payload["contact_name"]  = contact.get("name", "")
    payload["contact_email"] = contact.get("email", "")
    payload["stage"]         = contact.get("stage", "")
    # Build the full list of templates the user can pick from. Anything
    # matching this contact's industry/role at any trigger, plus the
    # industry default ('' role) as a fallback set. Lets the user
    # easily switch from the bandit-picked variant to something else.
    from crm import list_templates as _list_t, EMAIL_TRIGGERS
    industry = contact.get("industry") or ""
    role     = contact.get("role") or ""
    options = []
    for t in _list_t():
        if t.get("industry") != industry:
            continue
        if t.get("role") not in (role, "", None):
            continue
        options.append({
            "id":            t["id"],
            "industry":      t["industry"],
            "role":          t["role"] or "",
            "trigger":       t["trigger"],
            "trigger_label": EMAIL_TRIGGERS.get(t["trigger"], ""),
            "variant_label": t.get("variant_label") or "A",
            "subject":       t["subject"],
            "body":          t["body"],
            "sends_count":   t.get("sends_count") or 0,
            "replies_count": t.get("replies_count") or 0,
        })
    payload["all_templates"] = options
    return JSONResponse(payload)


@app.get("/api/pipeline/agreement/{contact_id}")
async def api_get_agreement(request: Request, contact_id: int):
    """Returns the pilot agreement template + the contact's saved
    state so the modal can render."""
    if not _check_pipeline_access(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    from crm import (get_contact, PILOT_AGREEMENT_SECTIONS,
                     agreement_progress)
    contact = get_contact(contact_id)
    if not contact:
        return JSONResponse({"error": "not found"}, status_code=404)
    saved = contact.get("pilot_agreement") or ""
    return JSONResponse({
        "contact_name":  contact.get("name"),
        "sections":      PILOT_AGREEMENT_SECTIONS,
        "saved":         saved,
        "progress":      agreement_progress(saved),
    })


@app.post("/api/pipeline/agreement/save")
async def api_save_agreement(request: Request):
    """Persist the pilot_agreement JSON. The frontend posts the entire
    blob each call (the modal is small enough that this is fine)."""
    if not _check_pipeline_access(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    import json as _json
    from crm import save_pilot_agreement, agreement_progress
    body = await request.json()
    try:
        contact_id = int(body.get("contact_id") or 0)
    except (TypeError, ValueError):
        contact_id = 0
    if not contact_id:
        return JSONResponse({"error": "missing contact_id"}, status_code=400)
    agreement = body.get("agreement") or {}
    if not isinstance(agreement, dict):
        return JSONResponse({"error": "agreement must be an object"},
                            status_code=400)
    blob = _json.dumps(agreement, separators=(",", ":"))
    save_pilot_agreement(contact_id, blob)
    return JSONResponse({"ok": True, "progress": agreement_progress(blob)})


@app.get("/api/pipeline/vercel/config")
async def api_vercel_config(request: Request):
    if not _check_pipeline_access(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    from vercel import configured as _vc
    from github_api import configured as _gc, get_authenticated_user
    gh_user = None
    if _gc():
        u = get_authenticated_user()
        if u: gh_user = u.get("login")
    return JSONResponse({
        "configured":        bool(_vc()),
        "github_configured": bool(_gc()),
        "github_user":       gh_user,
    })


@app.post("/api/pipeline/vercel/create")
async def api_vercel_create(request: Request):
    """Spin up a Vercel project + (optionally) a fresh GitHub repo for
    a contact. Auto-adds a Testing-page prototype entry pointing at
    the default .vercel.app URL. Returns a clone URL so the user can
    `git clone` and start Claude Code immediately."""
    if not _check_pipeline_access(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    from vercel import create_project, configured as _vc
    from github_api import (create_repo, configured as _gc)
    from crm import get_contact, add_prototype
    if not _vc():
        return JSONResponse({
            "error": ("Vercel sign-in not configured. Create a token at "
                      "https://vercel.com/account/tokens and add VERCEL_TOKEN "
                      "to Railway."),
        }, status_code=400)
    body = await request.json()
    try:
        contact_id = int(body.get("contact_id") or 0)
    except (TypeError, ValueError):
        contact_id = 0
    project_name  = (body.get("project_name") or "").strip()
    github_repo   = (body.get("github_repo") or "").strip() or None
    framework     = (body.get("framework") or "other").strip()
    proto_label   = (body.get("prototype_label") or "").strip()
    create_github = bool(body.get("create_github"))

    contact = get_contact(contact_id)
    if not contact:
        return JSONResponse({"error": "contact not found"}, status_code=404)
    if not project_name:
        project_name = f"{contact['name']}-prototype"

    # Step 1: create a fresh GitHub repo if requested
    github_result: dict | None = None
    if create_github and not github_repo:
        if not _gc():
            return JSONResponse({
                "error": ("Auto-create GitHub repo requested but GITHUB_TOKEN "
                          "is not set on Railway. Add a personal access token "
                          "with 'repo' scope from "
                          "https://github.com/settings/tokens/new"),
            }, status_code=400)
        github_result = create_repo(
            name=project_name,
            description=f"Prototype for {contact['name']} ({contact.get('agency') or ''})".strip(" ()"),
            private=True,
        )
        if not github_result.get("ok"):
            return JSONResponse({
                "error": "GitHub repo creation failed: " + github_result.get("error", ""),
                "github_response": github_result.get("raw"),
            }, status_code=502)
        github_repo = github_result["full_name"]

    # Step 2: create the Vercel project (linked to repo if we have one)
    result = create_project(
        name=project_name, github_repo=github_repo, framework=framework,
    )
    if not result.get("ok"):
        # If we created a repo but Vercel failed, surface both pieces
        # so the user knows the repo is real and what to do next.
        return JSONResponse({
            "error":            result.get("error", "Vercel API failed"),
            "vercel_response":  result.get("vercel_response"),
            "github_created":   bool(github_result and github_result.get("ok")),
            "github_clone_url": github_result and github_result.get("clone_url"),
            "github_html_url":  github_result and github_result.get("html_url"),
        }, status_code=502)

    # Step 3: auto-add to Testing page
    label = proto_label or f"{contact['name']} prototype"
    description = (
        f"Vercel project: {result['name']}\n"
        f"GitHub: {github_repo or '(not linked)'}\n"
        f"Framework: {framework}"
    )
    proto_id = add_prototype(
        contact_id=contact_id,
        name=label,
        prototype_url=result["project_url"],
        status="BUILDING",
        description=description,
    )

    return JSONResponse({
        "ok":               True,
        "project_url":      result["project_url"],
        "project_name":     result["name"],
        "prototype_id":     proto_id,
        "github_repo":      github_repo,
        "github_clone_url": github_result and github_result.get("clone_url"),
        "github_html_url":  github_result and github_result.get("html_url"),
    })


@app.get("/pipeline/testing")
async def pipeline_testing(request: Request):
    """Testing view — list of prototypes the client can be shown."""
    if not _check_pipeline_access(request):
        return RedirectResponse("/sign-in?redirect=/pipeline/testing",
                                status_code=303)
    from crm import (list_prototypes, list_contacts,
                     PROTOTYPE_STATUSES, PROTOTYPE_STATUS_LABELS,
                     ensure_feedback_tokens)
    # Backfill feedback tokens for any prototype missing one (e.g.
    # created before this column existed). Idempotent + fast.
    ensure_feedback_tokens()
    contacts = [c for c in list_contacts() if c["stage"] != "LOST"]
    base_url = _public_base_url(request)
    return templates.TemplateResponse("pipeline_testing.html", {
        "request":          request,
        "prototypes":       list_prototypes(),
        "contacts":         contacts,
        "statuses":         PROTOTYPE_STATUSES,
        "status_labels":    PROTOTYPE_STATUS_LABELS,
        "base_url":         base_url,
    })


def _public_base_url(request: Request) -> str:
    """Build the public origin for outbound URLs (feedback links in
    emails, etc.). Honors x-forwarded-* for Railway HTTPS."""
    scheme = request.headers.get("x-forwarded-proto", request.url.scheme)
    host = request.headers.get("x-forwarded-host") or request.headers.get("host") or request.url.netloc
    return f"{scheme}://{host}"


@app.get("/feedback/{token}")
async def feedback_page(request: Request, token: str):
    """Public client-facing feedback form. No auth — the token IS the
    auth. Client submits text + optional screenshot."""
    from crm import find_prototype_by_token
    p = find_prototype_by_token(token)
    if not p:
        return templates.TemplateResponse("feedback.html", {
            "request": request, "prototype": None, "token": token,
        }, status_code=404)
    return templates.TemplateResponse("feedback.html", {
        "request": request, "prototype": p, "token": token,
    })


@app.post("/feedback/{token}")
async def feedback_submit(request: Request, token: str):
    """Accept a feedback submission. Appends to the prototype's
    feedback log + emails the team via Resend (if configured)."""
    from crm import (find_prototype_by_token, update_prototype,
                     send_via_resend, resend_configured, resend_from_address,
                     SENDER_NAME)
    import os as _os
    p = find_prototype_by_token(token)
    if not p:
        return JSONResponse({"error": "Invalid feedback link."}, status_code=404)
    form = await request.form()
    text = (form.get("feedback") or "").strip()
    sender_name = (form.get("name") or "").strip()
    sender_email = (form.get("email") or "").strip()
    if not text:
        return JSONResponse({"error": "Feedback text is required."}, status_code=400)

    # Append to the prototype's feedback log with attribution.
    who = sender_name or sender_email or "Anonymous"
    if sender_email and sender_name:
        who = f"{sender_name} <{sender_email}>"
    log_entry = f"From: {who}\n\n{text}"
    update_prototype(p["id"], append_feedback=log_entry)

    # Try to grab a screenshot file (image/*) for the email attachment.
    attachments: list[dict] = []
    try:
        upload = form.get("screenshot")
        if upload and hasattr(upload, "read"):
            raw = await upload.read()
            if raw and len(raw) <= 5 * 1024 * 1024:  # 5 MB cap
                import base64 as _b64
                fname = getattr(upload, "filename", "screenshot.png") or "screenshot.png"
                attachments.append({
                    "filename": fname,
                    "content": _b64.b64encode(raw).decode("ascii"),
                })
    except Exception:
        attachments = []

    # Notify the team via Resend.
    if resend_configured():
        notify_to = (_os.environ.get("ADMIN_EMAILS", "").split(",") or [""])[0].strip() \
                    or resend_from_address()
        base_url = _public_base_url(request)
        subject = f"[FocusedOps] Feedback on {p['name']} — from {who}"
        body = (
            f"New feedback from {who}\n"
            f"Prototype: {p['name']}\n"
            f"{('Prototype URL: ' + p['prototype_url']) if p.get('prototype_url') else ''}\n\n"
            f"--- Feedback ---\n{text}\n\n"
            f"Manage at: {base_url}/pipeline/testing\n\n"
            f"{SENDER_NAME}"
        )
        try:
            send_via_resend(
                to_email=notify_to,
                subject=subject,
                body=body,
                reply_to=sender_email or None,
                **({"attachments": attachments} if attachments else {}),
            )
        except TypeError:
            # send_via_resend may not yet accept attachments — fall
            # back to plain text without screenshot.
            send_via_resend(to_email=notify_to, subject=subject,
                            body=body, reply_to=sender_email or None)

    return RedirectResponse(f"/feedback/{token}?ok=1", status_code=303)


# CORS-friendly JSON endpoint for the embeddable widget. Same logic
# as the form POST above but accepts JSON + screenshot_b64. Permissive
# CORS because the widget runs from arbitrary prototype domains
# (localtunnel, Vercel, custom domains, …) — the security model is
# the unguessable feedback token, not origin allowlisting.
@app.options("/api/feedback/{token}")
async def feedback_api_preflight(token: str):
    return JSONResponse({"ok": True}, headers={
        "Access-Control-Allow-Origin":  "*",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
        "Access-Control-Max-Age":       "600",
    })


@app.post("/api/feedback/{token}")
async def feedback_api(request: Request, token: str):
    from crm import (find_prototype_by_token, update_prototype,
                     send_via_resend, resend_configured, resend_from_address,
                     SENDER_NAME)
    import os as _os
    import base64 as _b64

    cors_headers = {
        "Access-Control-Allow-Origin": "*",
    }
    p = find_prototype_by_token(token)
    if not p:
        return JSONResponse({"error": "Invalid feedback link."},
                            status_code=404, headers=cors_headers)
    try:
        body = await request.json()
    except Exception:
        body = {}
    text = (body.get("feedback") or "").strip()
    if not text:
        return JSONResponse({"error": "feedback required"},
                            status_code=400, headers=cors_headers)
    sender_name  = (body.get("name") or "").strip()
    sender_email = (body.get("email") or "").strip()
    page_url     = (body.get("page_url") or "").strip()

    who = sender_name or sender_email or "Anonymous"
    if sender_email and sender_name:
        who = f"{sender_name} <{sender_email}>"
    header_lines = [f"From: {who}"]
    if page_url:
        header_lines.append(f"Page: {page_url}")
    log_entry = "\n".join(header_lines) + "\n\n" + text
    update_prototype(p["id"], append_feedback=log_entry)

    attachments: list[dict] = []
    shot_b64 = body.get("screenshot_b64") or ""
    if shot_b64 and len(shot_b64) < 7_000_000:  # ~5 MB raw
        try:
            _b64.b64decode(shot_b64, validate=True)
            attachments.append({
                "filename": (body.get("screenshot_filename")
                             or "screenshot.png"),
                "content":  shot_b64,
            })
        except Exception:
            attachments = []

    if resend_configured():
        notify_to = (_os.environ.get("ADMIN_EMAILS", "").split(",") or [""])[0].strip() \
                    or resend_from_address()
        base_url = _public_base_url(request)
        subject = f"[FocusedOps] Feedback on {p['name']} — from {who}"
        body_lines = [
            f"New feedback from {who}",
            f"Prototype: {p['name']}",
        ]
        if page_url:
            body_lines.append(f"Page they were on: {page_url}")
        if p.get("prototype_url"):
            body_lines.append(f"Prototype URL: {p['prototype_url']}")
        body_lines.append("")
        body_lines.append("--- Feedback ---")
        body_lines.append(text)
        body_lines.append("")
        body_lines.append(f"Manage at: {base_url}/pipeline/testing")
        body_lines.append("")
        body_lines.append(SENDER_NAME)
        notify_body = "\n".join(body_lines)
        try:
            send_via_resend(
                to_email=notify_to,
                subject=subject,
                body=notify_body,
                reply_to=sender_email or None,
                attachments=attachments or None,
            )
        except Exception as e:
            # Don't fail the widget on email failure — the log is saved.
            print(f"[feedback-api] Resend notify failed: {e}", flush=True)

    return JSONResponse({"ok": True}, headers=cors_headers)


@app.post("/pipeline/testing/add")
async def pipeline_testing_add(request: Request):
    if not _check_pipeline_access(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    from crm import add_prototype, PROTOTYPE_STATUSES
    form = await request.form()
    try:
        cid_raw = (form.get("contact_id") or "").strip()
        cid = int(cid_raw) if cid_raw else None
    except ValueError:
        cid = None
    name = (form.get("name") or "").strip()
    if not name:
        return RedirectResponse("/pipeline/testing", status_code=303)
    status = (form.get("status") or "BUILDING").strip()
    if status not in PROTOTYPE_STATUSES:
        status = "BUILDING"
    add_prototype(
        contact_id=cid,
        name=name,
        prototype_url=(form.get("prototype_url") or "").strip() or None,
        status=status,
        description=(form.get("description") or "").strip() or None,
    )
    return RedirectResponse("/pipeline/testing", status_code=303)


@app.post("/pipeline/testing/update")
async def pipeline_testing_update(request: Request):
    """Patch a prototype. Used by inline edits + feedback-append."""
    if not _check_pipeline_access(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    from crm import update_prototype
    body = await request.json()
    try:
        pid = int(body.get("id") or 0)
    except (TypeError, ValueError):
        pid = 0
    if not pid:
        return JSONResponse({"error": "missing id"}, status_code=400)
    update_prototype(
        pid,
        name=body.get("name"),
        prototype_url=body.get("prototype_url"),
        status=body.get("status"),
        description=body.get("description"),
        notes=body.get("notes"),
        append_feedback=body.get("append_feedback"),
    )
    return JSONResponse({"ok": True})


@app.post("/pipeline/testing/delete")
async def pipeline_testing_delete(request: Request):
    if not _check_pipeline_access(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    from crm import delete_prototype
    form = await request.form()
    try:
        pid = int(form.get("prototype_id", "0"))
    except ValueError:
        pid = 0
    if pid:
        delete_prototype(pid)
    return RedirectResponse("/pipeline/testing", status_code=303)


@app.get("/pipeline/templates")
async def pipeline_templates(request: Request):
    if not _check_pipeline_access(request):
        return RedirectResponse("/sign-in?redirect=/pipeline/templates",
                                status_code=303)
    from crm import INDUSTRIES, EMAIL_TRIGGERS, ROLES, list_templates
    return templates.TemplateResponse("pipeline_templates.html", {
        "request": request,
        "industries": INDUSTRIES,
        "email_triggers": EMAIL_TRIGGERS,
        "roles": ROLES,
        "templates_list": list_templates(),
    })


@app.post("/pipeline/template")
async def pipeline_save_template(request: Request):
    if not _check_pipeline_access(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    from crm import upsert_template
    form = await request.form()
    upsert_template(
        industry=(form.get("industry") or "").strip(),
        role=(form.get("role") or "").strip(),
        trigger=(form.get("trigger") or "").strip(),
        subject=(form.get("subject") or "").strip(),
        body=(form.get("body") or "").strip(),
        variant_label=(form.get("variant_label") or "").strip() or None,
    )
    return RedirectResponse("/pipeline/templates", status_code=303)


@app.post("/pipeline/template/delete")
async def pipeline_delete_template(request: Request):
    if not _check_pipeline_access(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    from crm import delete_template
    form = await request.form()
    try:
        tid = int(form.get("template_id", "0"))
    except ValueError:
        tid = 0
    if tid:
        delete_template(tid)
    return RedirectResponse("/pipeline/templates", status_code=303)


# ─── Discovery-call endpoints ────────────────────────────────────────
@app.get("/api/pipeline/call/{contact_id}")
async def api_pipeline_call_get(request: Request, contact_id: int):
    """Return the call payload for a contact: agenda, the four
    pre-filled prompts (transcript + extraction_json substituted in),
    and any saved artifacts + scorecard."""
    if not _check_pipeline_access(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    from crm import (DISCOVERY_AGENDA, DISCOVERY_PROMPT_EXTRACT,
                     DISCOVERY_PROMPT_EXEC_SUMMARY,
                     DISCOVERY_PROMPT_PAIN, DISCOVERY_PROMPT_MVP,
                     SCORECARD_DIMENSIONS, get_contact,
                     get_call_for_contact, render_prompt)
    contact = get_contact(contact_id)
    if not contact:
        return JSONResponse({"error": "not found"}, status_code=404)
    call = get_call_for_contact(contact_id) or {}
    transcript = call.get("transcript") or ""
    extraction = call.get("extraction_json") or ""

    import json as _json
    scorecard = None
    if call.get("scorecard_json"):
        try:
            scorecard = _json.loads(call["scorecard_json"])
        except (ValueError, TypeError):
            scorecard = None

    return JSONResponse({
        "contact_name":   contact.get("name", ""),
        "contact_email":  contact.get("email", ""),
        "agenda":         DISCOVERY_AGENDA,
        "prompts": {
            "extract":      render_prompt(DISCOVERY_PROMPT_EXTRACT, transcript=transcript),
            "exec_summary": render_prompt(DISCOVERY_PROMPT_EXEC_SUMMARY, extraction_json=extraction),
            "pain":         render_prompt(DISCOVERY_PROMPT_PAIN, extraction_json=extraction),
            "mvp":          render_prompt(DISCOVERY_PROMPT_MVP, extraction_json=extraction),
        },
        "scorecard_dimensions": [
            {"key": k, "weight": w, "label": label}
            for (k, w, label) in SCORECARD_DIMENSIONS
        ],
        "saved": {
            "call_date":       call.get("call_date").isoformat() if call.get("call_date") else "",
            "transcript":      transcript,
            "extraction_json": extraction,
            "exec_summary":    call.get("exec_summary") or "",
            "pain_analysis":   call.get("pain_analysis") or "",
            "mvp_scope":       call.get("mvp_scope") or "",
            "suggested_stage": call.get("suggested_stage") or "",
            "scorecard":       scorecard,
        },
    })


@app.post("/pipeline/call")
async def pipeline_save_call(request: Request):
    if not _check_pipeline_access(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    from crm import upsert_call
    from datetime import datetime as _dt, date as _date
    form = await request.form()
    try:
        contact_id = int(form.get("contact_id", "0"))
    except ValueError:
        contact_id = 0
    if not contact_id:
        return JSONResponse({"error": "missing contact_id"}, status_code=400)

    call_date = None
    raw = (form.get("call_date") or "").strip()
    if raw:
        try:
            call_date = _dt.strptime(raw, "%Y-%m-%d").date()
        except ValueError:
            call_date = None
    if call_date is None:
        call_date = _date.today()

    sc = upsert_call(
        contact_id=contact_id,
        call_date=call_date,
        transcript=(form.get("transcript") or "").strip(),
        extraction_json=(form.get("extraction_json") or "").strip(),
        exec_summary=(form.get("exec_summary") or "").strip(),
        pain_analysis=(form.get("pain_analysis") or "").strip(),
        mvp_scope=(form.get("mvp_scope") or "").strip(),
    )
    return JSONResponse({"ok": True, "scorecard": sc})


@app.post("/pipeline/call/delete")
async def pipeline_delete_call(request: Request):
    if not _check_pipeline_access(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    from crm import delete_call
    form = await request.form()
    try:
        contact_id = int(form.get("contact_id", "0"))
    except ValueError:
        contact_id = 0
    if contact_id:
        delete_call(contact_id)
    return JSONResponse({"ok": True})


# ─── Working-session endpoints (PILOT-stage pressure-test) ──────────
@app.get("/api/pipeline/session/{contact_id}")
async def api_pipeline_session_get(request: Request, contact_id: int):
    """Return the working-session payload for a contact: agenda, four
    pre-filled prompts, saved artifacts, and the scorecard band."""
    if not _check_pipeline_access(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    from crm import (WORKING_AGENDA, WORKING_PROMPT_EXTRACT,
                     WORKING_PROMPT_LOCKED_SCOPE, WORKING_PROMPT_CRITERIA,
                     WORKING_PROMPT_PROPOSAL, WORKING_PROMPT_PROTOTYPE,
                     WORKING_SCORECARD_DIMENSIONS,
                     get_contact, get_session_for_contact, render_prompt)
    contact = get_contact(contact_id)
    if not contact:
        return JSONResponse({"error": "not found"}, status_code=404)
    session = get_session_for_contact(contact_id) or {}
    transcript = session.get("transcript") or ""
    extraction = session.get("extraction_json") or ""
    locked_scope = session.get("locked_scope") or ""
    success_criteria = session.get("success_criteria") or ""
    email_thread = contact.get("email_thread") or ""

    import json as _json
    scorecard = None
    if session.get("scorecard_json"):
        try:
            scorecard = _json.loads(session["scorecard_json"])
        except (ValueError, TypeError):
            scorecard = None

    return JSONResponse({
        "contact_name":  contact.get("name", ""),
        "contact_email": contact.get("email", ""),
        "agenda":        WORKING_AGENDA,
        "prompts": {
            "extract":          render_prompt(WORKING_PROMPT_EXTRACT, transcript=transcript),
            "locked_scope":     render_prompt(WORKING_PROMPT_LOCKED_SCOPE, extraction_json=extraction),
            "criteria":         render_prompt(WORKING_PROMPT_CRITERIA, extraction_json=extraction),
            "proposal":         render_prompt(WORKING_PROMPT_PROPOSAL, extraction_json=extraction),
            "prototype":        render_prompt(WORKING_PROMPT_PROTOTYPE,
                                              extraction_json=extraction,
                                              locked_scope=locked_scope,
                                              success_criteria=success_criteria,
                                              email_thread=email_thread),
        },
        "scorecard_dimensions": [
            {"key": k, "weight": w, "label": label}
            for (k, w, label) in WORKING_SCORECARD_DIMENSIONS
        ],
        "saved": {
            "session_date":            session.get("session_date").isoformat() if session.get("session_date") else "",
            "transcript":              transcript,
            "extraction_json":         extraction,
            "locked_scope":            locked_scope,
            "success_criteria":        success_criteria,
            "proposal_draft":          session.get("proposal_draft") or "",
            "prototype_brief":         session.get("prototype_brief") or "",
            "iteration_feedback":      session.get("iteration_feedback") or "",
            "iteration_code_prompt":   session.get("iteration_code_prompt") or "",
            "iteration_design_prompt": session.get("iteration_design_prompt") or "",
            "suggested_action":        session.get("suggested_action") or "",
            "scorecard":               scorecard,
        },
    })


@app.post("/pipeline/session")
async def pipeline_save_session(request: Request):
    if not _check_pipeline_access(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    from crm import upsert_session
    from datetime import datetime as _dt, date as _date
    form = await request.form()
    try:
        cid = int(form.get("contact_id", "0"))
    except ValueError:
        cid = 0
    if not cid:
        return JSONResponse({"error": "missing contact_id"}, status_code=400)

    session_date = None
    raw = (form.get("session_date") or "").strip()
    if raw:
        try:
            session_date = _dt.strptime(raw, "%Y-%m-%d").date()
        except ValueError:
            session_date = None
    if session_date is None:
        session_date = _date.today()

    sc = upsert_session(
        contact_id=cid,
        session_date=session_date,
        transcript=(form.get("transcript") or "").strip(),
        extraction_json=(form.get("extraction_json") or "").strip(),
        locked_scope=(form.get("locked_scope") or "").strip(),
        success_criteria=(form.get("success_criteria") or "").strip(),
        proposal_draft=(form.get("proposal_draft") or "").strip(),
        prototype_brief=(form.get("prototype_brief") or "").strip(),
        iteration_feedback=(form.get("iteration_feedback") or "").strip(),
        iteration_code_prompt=(form.get("iteration_code_prompt") or "").strip(),
        iteration_design_prompt=(form.get("iteration_design_prompt") or "").strip(),
    )
    return JSONResponse({"ok": True, "scorecard": sc})


@app.post("/pipeline/session/delete")
async def pipeline_delete_session(request: Request):
    if not _check_pipeline_access(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    from crm import delete_session
    form = await request.form()
    try:
        cid = int(form.get("contact_id", "0"))
    except ValueError:
        cid = 0
    if cid:
        delete_session(cid)
    return JSONResponse({"ok": True})


# ─── Auto-process via Anthropic API ────────────────────────────────
@app.get("/api/pipeline/ai-config")
async def api_pipeline_ai_config(request: Request):
    """Expose which optional integrations are wired up. Used by the
    modals to show / hide the 'Auto-process with AI' button and the
    'Send via Resend' button."""
    if not _check_pipeline_access(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    from crm import (anthropic_configured, ANTHROPIC_MODEL,
                     resend_configured, resend_from_address)
    return JSONResponse({
        "anthropic_configured": anthropic_configured(),
        "model": ANTHROPIC_MODEL,
        "resend_configured":    resend_configured(),
        "resend_from":          resend_from_address() if resend_configured() else "",
    })


@app.post("/api/pipeline/send-email")
async def api_pipeline_send_email(request: Request):
    """Send a transactional email via Resend for a CRM contact. On
    success: appends the email to the contact's email_thread, bumps
    a QUEUED contact to CONTACTED, and sets date_emailed=today."""
    if not _check_pipeline_access(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    body = await request.json()
    try:
        contact_id = int(body.get("contact_id") or 0)
    except (TypeError, ValueError):
        contact_id = 0
    subject = (body.get("subject") or "").strip()
    body_text = (body.get("body") or "").strip()
    scheduled_at = (body.get("scheduled_at") or "").strip() or None
    try:
        template_id = int(body.get("template_id") or 0) or None
    except (TypeError, ValueError):
        template_id = None
    if not contact_id or not subject or not body_text:
        return JSONResponse({"error": "missing contact_id / subject / body"},
                            status_code=400)

    from crm import (get_contact, send_via_resend, update_contact,
                     change_stage, resend_configured, SENDER_NAME,
                     record_email_send)
    if not resend_configured():
        return JSONResponse({"error": "RESEND_API_KEY not set"}, status_code=400)

    contact = get_contact(contact_id)
    if not contact:
        return JSONResponse({"error": "contact not found"}, status_code=404)
    to_email = (contact.get("email") or "").strip()
    if not to_email:
        return JSONResponse({"error": "contact has no email address"},
                            status_code=400)

    body_text = body_text.replace("{my_name}", SENDER_NAME)
    sig_first_line = SENDER_NAME.split("\n", 1)[0].strip()
    if sig_first_line and sig_first_line not in body_text:
        body_text = body_text.rstrip() + "\n\n" + SENDER_NAME

    result = send_via_resend(
        to_email=to_email,
        subject=subject,
        body=body_text,
        scheduled_at=scheduled_at,
    )
    if not result.get("ok"):
        return JSONResponse({"error": result.get("error", "send failed")},
                            status_code=502)

    # Append to email_thread with a timestamp marker.
    from datetime import datetime as _dt, date as _date
    ts = _dt.now().strftime("%Y-%m-%d %H:%M")
    if scheduled_at:
        marker = f"--- Scheduled for {scheduled_at} (queued {ts}, " \
                 f"via Resend, id={result.get('id','')}) ---"
    else:
        marker = f"--- Sent {ts} (via Resend, id={result.get('id','')}) ---"
    entry = f"{marker}\nSubject: {subject}\n\n{body_text}"
    prev = (contact.get("email_thread") or "").strip()
    new_thread = (prev + "\n\n" + entry).strip() if prev else entry
    update_contact(
        contact_id,
        email_thread=new_thread,
        date_emailed=_date.today(),
    )
    if (contact.get("stage") or "") == "QUEUED":
        change_stage(contact_id, "CONTACTED")
    # A/B: record the send keyed to the variant used, so a future
    # transition into REPLIED can be attributed to this variant.
    record_email_send(contact_id, template_id)

    return JSONResponse({
        "ok": True,
        "id": result.get("id", ""),
        "scheduled_at": scheduled_at or "",
        "stage_advanced": (contact.get("stage") or "") == "QUEUED",
    })


@app.get("/api/pipeline/ab/stats")
async def api_ab_stats(request: Request):
    """A/B testing stats — grouped by (industry, role, trigger).
    Backs the Templates page Performance section."""
    if not _check_pipeline_access(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    from crm import variant_stats_grouped
    return JSONResponse({"groups": variant_stats_grouped()})


@app.post("/api/pipeline/ab/analyze")
async def api_ab_analyze(request: Request):
    """Run Claude on a specific (industry, role, trigger) group's
    stats. Returns markdown analysis + a parsed next-variant suggestion."""
    if not _check_pipeline_access(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    from crm import ab_analyze_group
    body = await request.json()
    industry = (body.get("industry") or "").strip()
    role     = (body.get("role") or "").strip()
    trigger  = (body.get("trigger") or "").strip()
    if not industry or not trigger:
        return JSONResponse({"error": "industry + trigger required"},
                            status_code=400)
    try:
        result = ab_analyze_group(industry, role, trigger)
        return JSONResponse({"ok": True, **result})
    except RuntimeError as e:
        return JSONResponse({"error": str(e)}, status_code=400)
    except Exception as e:
        logger.exception("ab analyze failed")
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/pipeline/ab/accept-variant")
async def api_ab_accept_variant(request: Request):
    """Persist the AI-suggested next variant as a new ACTIVE template."""
    if not _check_pipeline_access(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    from crm import upsert_template
    body = await request.json()
    industry = (body.get("industry") or "").strip()
    role     = (body.get("role") or "").strip()
    trigger  = (body.get("trigger") or "").strip()
    subject  = (body.get("subject") or "").strip()
    body_text = (body.get("body") or "").strip()
    variant_label = (body.get("variant_label") or "").strip()
    if not industry or not trigger or not subject or not body_text:
        return JSONResponse({"error": "missing required fields"}, status_code=400)
    ok = upsert_template(industry=industry, role=role, trigger=trigger,
                         subject=subject, body=body_text,
                         variant_label=variant_label or None)
    if not ok:
        return JSONResponse({"error": "save failed"}, status_code=400)
    return JSONResponse({"ok": True})


def _parse_iso_date(s: str, default):
    from datetime import datetime as _dt
    raw = (s or "").strip()
    if not raw:
        return default
    try:
        return _dt.strptime(raw, "%Y-%m-%d").date()
    except ValueError:
        return default


@app.post("/api/pipeline/call/{contact_id}/auto")
async def api_pipeline_call_auto(request: Request, contact_id: int):
    """Run the full discovery-call chain via Claude API as a streamed
    NDJSON response so the UI can show per-step progress. Each line
    is a JSON object: {"step":"…","label":"…"} for progress, then a
    final {"done": true, "extraction_json": …, ...} payload."""
    if not _check_pipeline_access(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    body = await request.json()
    transcript = (body.get("transcript") or "").strip()
    if not transcript:
        return JSONResponse({"error": "missing transcript"}, status_code=400)
    from datetime import date as _date
    call_date = _parse_iso_date(body.get("call_date") or "", _date.today())

    async def gen():
        import json as _json
        import asyncio as _asyncio
        from concurrent.futures import ThreadPoolExecutor
        from crm import (call_claude, _strip_code_fence, render_prompt,
                         DISCOVERY_PROMPT_EXTRACT, DISCOVERY_PROMPT_EXEC_SUMMARY,
                         DISCOVERY_PROMPT_PAIN, DISCOVERY_PROMPT_MVP,
                         upsert_call)

        def emit(obj):
            return (_json.dumps(obj) + "\n").encode("utf-8")

        try:
            yield emit({"step": "extract", "label": "Step 1/3 — Extracting structured data"})
            extraction = await _asyncio.to_thread(
                lambda: _strip_code_fence(call_claude(
                    render_prompt(DISCOVERY_PROMPT_EXTRACT, transcript=transcript),
                    max_tokens=8192,
                ))
            )

            yield emit({"step": "artifacts",
                        "label": "Step 2/3 — Generating exec summary, pain analysis, MVP scope (in parallel)"})

            def run_artifacts():
                prompts = [
                    ("exec_summary",  render_prompt(DISCOVERY_PROMPT_EXEC_SUMMARY, extraction_json=extraction)),
                    ("pain_analysis", render_prompt(DISCOVERY_PROMPT_PAIN,         extraction_json=extraction)),
                    ("mvp_scope",     render_prompt(DISCOVERY_PROMPT_MVP,          extraction_json=extraction)),
                ]
                with ThreadPoolExecutor(max_workers=3) as ex:
                    futures = {k: ex.submit(call_claude, p, max_tokens=2048) for k, p in prompts}
                    return {k: f.result().strip() for k, f in futures.items()}

            outputs = await _asyncio.to_thread(run_artifacts)

            yield emit({"step": "save", "label": "Step 3/3 — Saving artifacts + scoring"})
            sc = await _asyncio.to_thread(upsert_call,
                contact_id=contact_id,
                call_date=call_date,
                transcript=transcript,
                extraction_json=extraction,
                exec_summary=outputs["exec_summary"],
                pain_analysis=outputs["pain_analysis"],
                mvp_scope=outputs["mvp_scope"],
            )

            yield emit({
                "done": True,
                "extraction_json": extraction,
                "exec_summary":    outputs["exec_summary"],
                "pain_analysis":   outputs["pain_analysis"],
                "mvp_scope":       outputs["mvp_scope"],
                "scorecard":       sc,
            })
        except RuntimeError as e:
            yield emit({"error": str(e)})
        except Exception as e:
            logger.exception("discovery auto-process failed")
            yield emit({"error": str(e)})

    from fastapi.responses import StreamingResponse
    return StreamingResponse(gen(), media_type="application/x-ndjson")


@app.post("/api/pipeline/session/{contact_id}/auto")
async def api_pipeline_session_auto(request: Request, contact_id: int):
    """Same streamed shape as the discovery-call auto endpoint, but
    for the working-session chain."""
    if not _check_pipeline_access(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    body = await request.json()
    transcript = (body.get("transcript") or "").strip()
    if not transcript:
        return JSONResponse({"error": "missing transcript"}, status_code=400)
    from datetime import date as _date
    session_date = _parse_iso_date(body.get("session_date") or "", _date.today())

    async def gen():
        import json as _json
        import asyncio as _asyncio
        from concurrent.futures import ThreadPoolExecutor
        from crm import (call_claude, _strip_code_fence, render_prompt,
                         WORKING_PROMPT_EXTRACT, WORKING_PROMPT_LOCKED_SCOPE,
                         WORKING_PROMPT_CRITERIA, WORKING_PROMPT_PROPOSAL,
                         WORKING_PROMPT_PROTOTYPE,
                         get_contact, upsert_session)

        def emit(obj):
            return (_json.dumps(obj) + "\n").encode("utf-8")

        try:
            yield emit({"step": "extract", "label": "Step 1/4 — Extracting structured data"})
            extraction = await _asyncio.to_thread(
                lambda: _strip_code_fence(call_claude(
                    render_prompt(WORKING_PROMPT_EXTRACT, transcript=transcript),
                    max_tokens=8192,
                ))
            )

            yield emit({"step": "artifacts",
                        "label": "Step 2/4 — Generating locked scope, success criteria, proposal draft (parallel)"})

            def run_artifacts():
                prompts = [
                    ("locked_scope",     render_prompt(WORKING_PROMPT_LOCKED_SCOPE, extraction_json=extraction)),
                    ("success_criteria", render_prompt(WORKING_PROMPT_CRITERIA,    extraction_json=extraction)),
                    ("proposal_draft",   render_prompt(WORKING_PROMPT_PROPOSAL,    extraction_json=extraction)),
                ]
                with ThreadPoolExecutor(max_workers=3) as ex:
                    futures = {k: ex.submit(call_claude, p, max_tokens=2048) for k, p in prompts}
                    return {k: f.result().strip() for k, f in futures.items()}

            outputs = await _asyncio.to_thread(run_artifacts)

            yield emit({"step": "prototype",
                        "label": "Step 3/4 — Generating prototype build brief (pulls in email_thread too)"})

            # Pull email_thread from contact record so the prototype
            # brief has the full async context.
            email_thread = ""
            try:
                contact = await _asyncio.to_thread(get_contact, contact_id)
                if contact:
                    email_thread = contact.get("email_thread") or ""
            except Exception:
                pass

            prototype_brief = await _asyncio.to_thread(
                lambda: call_claude(
                    render_prompt(
                        WORKING_PROMPT_PROTOTYPE,
                        extraction_json=extraction,
                        locked_scope=outputs["locked_scope"],
                        success_criteria=outputs["success_criteria"],
                        email_thread=email_thread,
                    ),
                    max_tokens=4096,
                ).strip()
            )

            yield emit({"step": "save", "label": "Step 4/4 — Saving artifacts + scoring"})
            sc = await _asyncio.to_thread(upsert_session,
                contact_id=contact_id,
                session_date=session_date,
                transcript=transcript,
                extraction_json=extraction,
                locked_scope=outputs["locked_scope"],
                success_criteria=outputs["success_criteria"],
                proposal_draft=outputs["proposal_draft"],
                prototype_brief=prototype_brief,
            )

            yield emit({
                "done": True,
                "extraction_json":  extraction,
                "locked_scope":     outputs["locked_scope"],
                "success_criteria": outputs["success_criteria"],
                "proposal_draft":   outputs["proposal_draft"],
                "prototype_brief":  prototype_brief,
                "scorecard":        sc,
            })
        except RuntimeError as e:
            yield emit({"error": str(e)})
        except Exception as e:
            logger.exception("working-session auto-process failed")
            yield emit({"error": str(e)})

    from fastapi.responses import StreamingResponse
    return StreamingResponse(gen(), media_type="application/x-ndjson")


@app.get("/api/pipeline/session/{contact_id}/iteration-prompt")
async def api_pipeline_iteration_prompt(request: Request, contact_id: int):
    """Returns the rendered meta-prompt for Step 6 (paste-into-claude.ai
    fallback when ANTHROPIC_API_KEY isn't set). The UI shows this so
    the user can copy it manually."""
    if not _check_pipeline_access(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    feedback = (request.query_params.get("feedback") or "").strip()
    from crm import (WORKING_PROMPT_ITERATION, render_prompt,
                     get_session_for_contact, list_prototypes)
    session = get_session_for_contact(contact_id) or {}
    protos  = [p for p in list_prototypes() if p.get("contact_id") == contact_id]
    proto   = protos[0] if protos else {}
    prompt  = render_prompt(
        WORKING_PROMPT_ITERATION,
        locked_scope=session.get("locked_scope") or "",
        success_criteria=session.get("success_criteria") or "",
        iteration_feedback=feedback,
        prototype_name=proto.get("name") or "",
        prototype_url=proto.get("prototype_url") or "",
        prototype_description=proto.get("description") or "",
    )
    return JSONResponse({"prompt": prompt})


@app.post("/api/pipeline/session/{contact_id}/iteration/auto")
async def api_pipeline_iteration_auto(request: Request, contact_id: int):
    """Step 6 — auto-process: takes free-form client feedback, sends
    the meta-prompt to Claude, returns the two split prompts (Claude
    Code + claude.ai design). Streams NDJSON progress events like the
    other auto endpoints."""
    if not _check_pipeline_access(request):
        return JSONResponse({"error": "unauthorized"}, status_code=403)
    body = await request.json()
    feedback = (body.get("feedback") or "").strip()
    if not feedback:
        return JSONResponse({"error": "missing feedback"}, status_code=400)

    async def gen():
        import json as _json
        import asyncio as _asyncio
        from crm import process_iteration_auto

        def emit(obj):
            return (_json.dumps(obj) + "\n").encode("utf-8")

        try:
            yield emit({"step": "iterate",
                        "label": "Step 6 — Asking Claude to think like a senior engineer + designer…"})
            out = await _asyncio.to_thread(
                process_iteration_auto, contact_id, feedback,
            )
            yield emit({
                "done": True,
                "iteration_feedback":      out["iteration_feedback"],
                "iteration_code_prompt":   out["iteration_code_prompt"],
                "iteration_design_prompt": out["iteration_design_prompt"],
            })
        except RuntimeError as e:
            yield emit({"error": str(e)})
        except Exception as e:
            logger.exception("Step 6 iteration auto-process failed")
            yield emit({"error": str(e)})

    from fastapi.responses import StreamingResponse
    return StreamingResponse(gen(), media_type="application/x-ndjson")


# ─── Stock lookup (public) ──────────────────────────────────────────
# Single-ticker search: Yahoo Finance for live quote + 1Y chart, SEC
# EDGAR for latest annual fundamentals. Public — no admin gate.

@app.get("/stocks")
async def stocks_page(request: Request):
    return templates.TemplateResponse("stocks.html", {"request": request})


@app.get("/api/stock/{ticker}/quote")
async def api_stock_quote(ticker: str):
    from stock_lookup import get_quote
    # get_quote() makes a blocking Yahoo request — run it off the event
    # loop so one slow fetch doesn't stall every concurrent request.
    return JSONResponse(await asyncio.to_thread(get_quote, ticker))


@app.get("/api/stock/{ticker}/fundamentals")
async def api_stock_fundamentals(ticker: str):
    from stock_lookup import get_fundamentals
    return JSONResponse(await asyncio.to_thread(get_fundamentals, ticker))


# ─── Real Mortgage Payment Price Index (public) ─────────────────────
# Case-Shiller home prices, deflated by CPI-Less-Shelter, with the
# mortgage rate at each month baked in. The single best "is housing
# expensive right now?" chart we have. Originally John Wake's idea
# at RealEstateDecoded.com.

@app.get("/real-mortgage-index")
async def real_mortgage_index_page(request: Request):
    from real_mortgage_index import list_metros
    return templates.TemplateResponse("real_mortgage_index.html", {
        "request": request,
        "metros": list_metros(),
    })


def _fha_piti(home_value: float, state_code: str, rate_pct: float,
              down_pct: float = 3.5, years: int = 30) -> dict | None:
    """FHA-flavored PITI estimate. Mirrors qualifying_income() in
    data_providers (same state tables for property tax + insurance,
    same homestead exemption math) plus FHA's monthly mortgage
    insurance premium (MIP).

    MIP: 0.55% annual when LTV > 90% (i.e. any FHA loan with <10%
    down), divided by 12 for monthly. For loans originated post-2013
    with 3.5% down, MIP is required for the life of the loan, not
    just until 78% LTV. We model that — first-time FHA buyers should
    plan on it staying.

    Returns a dict with the components broken out so the UI can
    show the user where their money goes.
    """
    from data_providers import (
        STATE_PROPERTY_TAX_RATE, STATE_INSURANCE_ANNUAL,
        STATE_HOMESTEAD_EXEMPTION,
    )
    if not home_value or home_value <= 0 or not rate_pct or rate_pct <= 0:
        return None
    loan = home_value * (1 - down_pct / 100.0)
    r = (rate_pct / 100.0) / 12.0
    n = years * 12
    p_and_i = loan * (r * (1 + r) ** n) / ((1 + r) ** n - 1) if r > 0 else loan / n
    tax_rate = STATE_PROPERTY_TAX_RATE.get(state_code, 0.011)
    homestead = STATE_HOMESTEAD_EXEMPTION.get(state_code, 0)
    taxable = max(home_value - homestead, 0)
    monthly_tax = (taxable * tax_rate) / 12.0
    monthly_ins = STATE_INSURANCE_ANNUAL.get(state_code, 1800) / 12.0
    monthly_mip = (loan * 0.0055) / 12.0 if down_pct < 10 else 0.0
    return {
        "loan": loan,
        "p_and_i": p_and_i,
        "monthly_tax": monthly_tax,
        "monthly_ins": monthly_ins,
        "monthly_mip": monthly_mip,
        "piti": p_and_i + monthly_tax + monthly_ins + monthly_mip,
        "down_cash": home_value * (down_pct / 100.0),
        # Estimate closing costs at 3% of price — FHA buyers
        # sometimes roll these into the loan, sometimes don't. Show
        # the un-rolled number so users see total cash to close.
        "closing_est": home_value * 0.03,
    }


@app.get("/multifamily")
async def multifamily_page(
    request: Request,
    state: str = "OH",
    max_price: int = 550000,
    down_pct: float = 3.5,
    units: int = 2,
):
    """Multifamily ZIP scout, tuned for first-time FHA owner-occupants.

    Default frame: a small-and-mighty investor with ~3.5% FHA cash
    buying a 2-4 unit under $550k and house-hacking (lives in one
    unit, rents the others). The page ranks ZIPs by signals that
    matter for THAT deal: cap rate (cash flow), house-hack net
    monthly cost (PITI minus rented-unit income), and — once ACS
    data lands — renter density, 2-4 unit stock share, and rent
    burden.

    Query params let users stress-test their own deal:
      state      — 2-letter code (default OH)
      max_price  — affordability cap in dollars (default 550000)
      down_pct   — FHA min is 3.5; conventional 5%+ for invest, 25%+
                   for non-owner-occupant
      units      — 2 / 3 / 4. Affects expected rent (n-1 units rented).

    The Census ACS multifamily columns (pct_renter_occupied,
    pct_multi_unit, pct_rent_burdened) populate on each monthly
    refresh-national-zips workflow run; before that lands, the
    page falls back to a 3-signal blend that's still useful."""
    import sqlite3
    from bisect import bisect_left
    from data_providers import MORTGAGE_30Y_RATE
    state = (state or "OH").upper()
    # Clamp inputs so a wonky URL param can't blow up the math.
    max_price = max(50_000, min(2_000_000, int(max_price or 550_000)))
    down_pct = max(0.0, min(50.0, float(down_pct or 3.5)))
    units = max(2, min(4, int(units or 2)))

    db_path = Path(__file__).resolve().parent / "data" / "zips.db"
    if not db_path.exists():
        return templates.TemplateResponse("multifamily.html", {
            "request": request, "rows": [], "state": state, "states": [],
            "has_mf_data": False, "data_pending": True,
            "max_price": max_price, "down_pct": down_pct, "units": units,
        })
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    states = [r[0] for r in cur.execute(
        "select distinct state from zips where state is not null and state != '' order by state"
    ).fetchall()]
    cols = {r[1] for r in cur.execute("PRAGMA table_info(zips)").fetchall()}
    has_mf_data = {"pct_renter_occupied", "pct_multi_unit", "pct_rent_burdened"} <= cols
    if has_mf_data:
        cur.execute("select count(*) from zips where pct_renter_occupied is not null")
        has_mf_data = cur.fetchone()[0] > 0

    # Affordability filter: median_home_value must be within
    # 1.2× of the user's cap. The 20% headroom accounts for the
    # fact that median is a midpoint — there are cheaper duplexes
    # *and* the user may stretch slightly above their target.
    # ZIPs >1.2× the cap are dropped entirely so the table only
    # shows places the user could plausibly shop in.
    hv_ceiling = int(max_price * 1.2)

    has_history = "history_zhvi" in cols
    hist_col = "history_zhvi" if has_history else "NULL"
    if has_mf_data:
        select_cols = f"""zip, name, neighborhood, lat, lng, population, population_density,
                   median_home_value, median_rent_monthly, cap_rate_pct,
                   pct_renter_occupied, pct_multi_unit, pct_rent_burdened,
                   walk_score, {hist_col}"""
    else:
        select_cols = f"""zip, name, neighborhood, lat, lng, population, population_density,
                   median_home_value, median_rent_monthly, cap_rate_pct,
                   NULL, NULL, NULL,
                   walk_score, {hist_col}"""
    rows_raw = cur.execute(f"""
        select {select_cols}
        from zips
        where state = ?
          and population >= 1500
          and median_home_value is not null
          and median_home_value <= ?
          and median_rent_monthly is not null
          and cap_rate_pct is not null
    """, (state, hv_ceiling)).fetchall()
    conn.close()

    # ── Per-row FHA house-hack math ───────────────────────────────
    # Purchase price = min(median home value, user's cap). This
    # assumes "I'll buy at or below the local median for a 2-4 unit
    # in this ZIP." Use the cap if median exceeds it (user stretches
    # to the cap); else use the median (cheaper than user's budget).
    # Expected rent = (units - 1) × median rent. Conservative
    # (assumes user rents one unit; we don't have per-unit-count rent
    # in our data, so this is a reasonable approximation).
    # Net cost = PITI - expected rent. Negative means the renters
    # pay more than your full housing cost — you live free + cash flow.
    from structural import (trajectory_from_history, durable_cap_rate,
                            state_structural, apply_trajectory_veto,
                            TRAJECTORY_BADGES)
    state_struct = state_structural(state)
    n_state_flags = len(state_struct["flags"])

    house_hack_costs = []
    stressed_costs = []   # rents −10%, insurance +30%, 1 month vacancy/yr
    fha_passes = []  # only meaningful for units>=3 per FHA rules
    trajectories = []
    durable_caps = []
    for r in rows_raw:
        hv, rent = r[7], r[8]
        # Value trajectory from the ZIP's 60-month ZHVI history — the
        # structural lens: a great level score with a declining series
        # gets vetoed below, not averaged away.
        traj = None
        if r[14]:
            try:
                traj = trajectory_from_history(json.loads(r[14]))
            except (ValueError, TypeError):
                traj = None
        trajectories.append(traj)
        durable = durable_cap_rate(r[9], hv, state)
        durable_caps.append(durable)

        purchase = min(hv, max_price)
        piti = _fha_piti(purchase, state, MORTGAGE_30Y_RATE, down_pct=down_pct)
        if not piti:
            house_hack_costs.append(None)
            stressed_costs.append(None)
            fha_passes.append(None)
            continue
        expected_rent = (units - 1) * rent
        net = piti["piti"] - expected_rent
        house_hack_costs.append(round(net))
        # Stress test: same deal if rents come in 10% light, insurance
        # reprices +30%, and you eat one vacant month per year. If THIS
        # number still cash-flows, the deal survives a structural shift.
        stressed_piti = piti["piti"] + 0.30 * piti["monthly_ins"]
        stressed_rent = expected_rent * 0.90 * (11 / 12)
        stressed_costs.append(round(stressed_piti - stressed_rent))
        # FHA self-sufficiency test (3-4 unit only): 75% of *total*
        # rents (all units, including owner-occupied) must cover PITI.
        if units >= 3:
            qualifying_rent = 0.75 * units * rent
            fha_passes.append(qualifying_rent >= piti["piti"])
        else:
            fha_passes.append(None)

    def rank_pct(values, invert=False):
        """Return per-row rank → percentile 0–100. invert=True flips
        so lower input values get higher percentiles (used for
        cost-style metrics where 'lower is better')."""
        valid = [v for v in values if v is not None]
        if not valid:
            return [None] * len(values)
        sorted_vals = sorted(valid)
        n = len(sorted_vals)
        out = []
        for v in values:
            if v is None:
                out.append(None); continue
            i = bisect_left(sorted_vals, v)
            pct = i / max(1, n - 1) * 100
            out.append(round(100 - pct, 1) if invert else round(pct, 1))
        return out

    cap_pcts  = rank_pct([r[9] for r in rows_raw])
    dens_pcts = rank_pct([r[6] for r in rows_raw])
    # House-hack net cost is inverted: lower monthly burn = higher
    # score. This is the single most important signal for a small,
    # cash-constrained investor — weighted at 35-40% below.
    hh_pcts = rank_pct(house_hack_costs, invert=True)

    if has_mf_data:
        renter_pcts = rank_pct([r[10] for r in rows_raw])
        multi_pcts  = rank_pct([r[11] for r in rows_raw])
        burden_inv  = rank_pct([r[12] for r in rows_raw], invert=True)
        # FHA-tuned weights: cap rate + house-hack cost dominate
        # because that's what the small-mighty owner-occupant cares
        # about. Renter % and multi-unit % matter but secondary.
        weights = (
            ("cap",    0.25),
            ("hh",     0.35),
            ("renter", 0.20),
            ("multi",  0.10),
            ("burden", 0.10),
        )
    else:
        renter_pcts = [None] * len(rows_raw)
        multi_pcts  = [None] * len(rows_raw)
        burden_inv  = [None] * len(rows_raw)
        walk_pcts = rank_pct([r[13] for r in rows_raw])
        # Fallback (no ACS yet): cap + house-hack still get the
        # bulk of the weight; density and walk fill in.
        weights = (
            ("cap",    0.35),
            ("hh",     0.40),
            ("density",0.15),
            ("walk",   0.10),
        )

    rows = []
    for i, r in enumerate(rows_raw):
        (zip_code, name, neigh, lat, lng, pop, dens, hv, rent, cap,
         pct_rent, pct_mu, pct_rb, walk, _hist) = r
        if hh_pcts[i] is None or cap_pcts[i] is None:
            continue
        if has_mf_data:
            inputs = {
                "cap":    cap_pcts[i],
                "hh":     hh_pcts[i],
                "renter": renter_pcts[i],
                "multi":  multi_pcts[i],
                "burden": burden_inv[i],
            }
        else:
            inputs = {
                "cap":     cap_pcts[i],
                "hh":      hh_pcts[i],
                "density": dens_pcts[i],
                "walk":    walk_pcts[i],
            }
        if any(inputs[k] is None for k, _ in weights):
            continue
        score = round(sum(inputs[k] * w for k, w in weights), 1)
        # Trajectory veto: a declining/decelerating ZIP can't ride a
        # cheap-level score to the top of the table. Vetoed rows are
        # marked so the UI shows *why* the score is capped.
        traj = trajectories[i]
        score, vetoed = apply_trajectory_veto(
            score, traj["label"] if traj else None, n_state_flags)
        durable = durable_caps[i]
        rows.append({
            "zip": zip_code, "name": name, "neighborhood": neigh or "",
            "lat": lat, "lng": lng, "population": pop,
            "median_home_value": hv, "median_rent_monthly": rent,
            "cap_rate_pct": cap,
            "pct_renter_occupied": pct_rent,
            "pct_multi_unit": pct_mu,
            "pct_rent_burdened": pct_rb,
            "house_hack_net": house_hack_costs[i],
            "house_hack_stressed": stressed_costs[i],
            "fha_self_suff": fha_passes[i],
            "mf_score": score,
            "vetoed": vetoed,
            "traj": traj,
            "traj_badge": TRAJECTORY_BADGES.get(traj["label"]) if traj else None,
            "durable_cap_pct": durable["durable_cap_pct"] if durable else None,
            "durable_detail": durable,
        })
    rows.sort(key=lambda r: r["mf_score"], reverse=True)

    # PITI breakdown at the user's exact cap — shown above the table
    # so they can see what their max-price scenario costs.
    sample_piti = _fha_piti(max_price, state, MORTGAGE_30Y_RATE, down_pct=down_pct)

    return templates.TemplateResponse("multifamily.html", {
        "request": request,
        "rows": rows[:100],
        "state": state, "states": states,
        "max_price": max_price, "down_pct": down_pct, "units": units,
        "sample_piti": sample_piti,
        "mortgage_rate": MORTGAGE_30Y_RATE,
        "has_mf_data": has_mf_data,
        "data_pending": not has_mf_data,
        "state_struct": state_struct,
        "has_history": has_history,
    })


@app.get("/fair-value")
async def fair_value_page(request: Request, state: str = "OH"):
    """Inflation-adjusted-payment fair-value methodology (per
    @VladTheInflator). Takes a state's median home value from ~5
    years ago, builds the baseline PITI, inflates the payment by
    cumulative CPI, then back-solves for the home price today's
    mortgage rate produces. Compares to current market value to
    flag % over/undervalued."""
    from data_providers import CHOROPLETH_STATES
    from fair_value import compute_state_fair_value, compute_zips_in_state
    state = (state or "OH").upper()
    rows = []
    for code, sd in CHOROPLETH_STATES.items():
        mv = sd.get("home_value")
        if not mv:
            continue
        result = compute_state_fair_value(code, mv)
        if not result:
            continue
        result["code"] = code
        result["name"] = sd.get("name", code)
        rows.append(result)
    rows.sort(key=lambda r: r["delta_pct"], reverse=True)
    picked = next((r for r in rows if r["code"] == state), None)
    # Per-ZIP drilldown for the picked state. Send everything — the
    # search box has to be able to find any ZIP, and capping the
    # response set hides ZIPs ranked below the cap (Lakewood OH
    # ranked #404 of 979; +56% overvalued but invisible at cap=250).
    # 5000-row hard ceiling is a safety net against pathological
    # states; CA has ~1500 ZIPs which is the realistic upper bound.
    zip_rows = compute_zips_in_state(state, limit=5000) if picked else []
    # FIPS→delta map for the choropleth — us-states.json features
    # are keyed by FIPS, not by 2-letter code. Pre-build the lookup
    # so the client doesn't have to do it for every polygon.
    fips_to_delta = {}
    for r in rows:
        fips = CHOROPLETH_STATES.get(r["code"], {}).get("fips")
        if fips:
            fips_to_delta[fips] = {
                "code": r["code"], "name": r["name"],
                "delta_pct": r["delta_pct"],
                "fair_value": r["fair_value"], "market_value": r["market_value"],
            }
    # ZIP markers — only need the geo + delta_pct for the map; the
    # full rows already power the table below.
    zip_markers = [
        {"zip": r["zip"], "lat": r["lat"], "lng": r["lng"],
         "delta_pct": r["delta_pct"], "area": r["area"],
         "market_value": r["market_value"], "fair_value": r["fair_value"]}
        for r in zip_rows if r.get("lat") is not None and r.get("lng") is not None
    ]
    return templates.TemplateResponse("fair_value.html", {
        "request": request,
        "rows": rows,
        "picked": picked,
        "zip_rows": zip_rows,
        "state": state,
        "states": sorted(r["code"] for r in rows),
        "fips_to_delta": fips_to_delta,
        "zip_markers": zip_markers,
    })


@app.get("/conditions")
async def conditions_page(request: Request):
    """Market Conditions dashboard — ranks states by the 4-signal
    Market Climate composite (sale-to-list, price drops, DOM, months
    of supply) so investors can see which markets are coolest for
    buyers at a glance. Data flows from data_providers' enriched
    CHOROPLETH_STATES (Redfin overrides + the _compute_market_climate
    pass that runs on module load)."""
    from data_providers import CHOROPLETH_STATES
    rows = []
    for code, sd in CHOROPLETH_STATES.items():
        # Skip states with missing inputs — the composite would be
        # None and the row would look broken next to populated ones.
        if sd.get("market_climate_pct") is None:
            continue
        rows.append({
            "code": code,
            "name": sd.get("name", code),
            "fips": sd.get("fips"),
            "market_climate_pct": sd["market_climate_pct"],
            "sale_to_list_pct": sd.get("sale_to_list_pct"),
            "price_drops_pct": sd.get("price_drops_pct"),
            "dom": sd.get("dom"),
            "months_of_supply": sd.get("months_of_supply"),
            "home_value": sd.get("home_value"),
            "home_value_yoy": sd.get("home_value_yoy"),
        })
    # Default sort: most buyer-friendly first.
    rows.sort(key=lambda r: r["market_climate_pct"], reverse=True)
    # Surface the Redfin period_end so users know how fresh the data
    # is — read it from the overrides file's _meta the same way the
    # rest of the site does.
    redfin_period_end = None
    try:
        from pathlib import Path
        p = Path(__file__).resolve().parent / "data" / "redfin_overrides.json"
        if p.exists():
            redfin_period_end = json.loads(p.read_text()).get("_meta", {}).get("primary_period_end")
    except Exception:
        pass
    return templates.TemplateResponse("conditions.html", {
        "request": request,
        "rows": rows,
        "redfin_period_end": redfin_period_end,
    })


@app.get("/api/real-mortgage-index")
async def api_real_mortgage_index(metro: str = "US", down_pct: float = 10.0):
    from real_mortgage_index import compute_index
    # compute_index() does three blocking FRED calls on a cache miss.
    return JSONResponse(await asyncio.to_thread(compute_index, metro, down_pct))


# ─── Sign-up (Phase 1 of paywall — email capture only) ──────────────
# Free for now. Captures email + optional name + source page so we
# can email people when paid features launch. No login UI, no
# password — Phase 2 will add magic-link auth when we actually need
# to gate features per user.

import re

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


@app.get("/signup")
async def signup_page(request: Request):
    return templates.TemplateResponse("signup.html", {"request": request})


@app.post("/api/signup")
async def api_signup(request: Request):
    """Insert a signup. Validates email format, rejects honeypot,
    de-dupes on email. Returns 201 on new signup, 200 on existing."""
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON body."}, status_code=400)

    # Honeypot — hidden field on the form. Real users never fill it,
    # bots fill it indiscriminately. Silent-200 (don't tell the bot
    # we caught it) so they don't adapt.
    if (body.get("website") or "").strip():
        return JSONResponse({"created": False, "ignored": True})

    email = (body.get("email") or "").strip().lower()
    name = (body.get("name") or "").strip() or None
    source = (body.get("source") or "/signup").strip()[:60]
    if not EMAIL_RE.match(email):
        return JSONResponse({"error": "Please enter a valid email."}, status_code=400)
    if len(email) > 255:
        return JSONResponse({"error": "Email is too long."}, status_code=400)

    user_agent = request.headers.get("user-agent", "")[:255]
    created, uid = add_user(email=email, name=name, source=source, user_agent=user_agent)
    status = 201 if created else 200
    return JSONResponse({"created": created, "id": uid}, status_code=status)


@app.get("/api/signups/count")
async def api_signups_count():
    """Public endpoint — useful for a 'join 1,247 others' badge."""
    return JSONResponse({"count": get_user_count()})


ADMIN_COOKIE = "mp_admin"


def _check_admin_token(request: Request) -> bool:
    """Admin access. Accepts (in order):
      • mp_session cookie with role=admin (Google OAuth path)
      • ?token=<ADMIN_TOKEN> query param
      • X-Admin-Token request header
      • mp_admin browser cookie set by /admin/login

    Returns False when neither path is satisfied."""
    # Google OAuth session path.
    from auth import SESSION_COOKIE, verify_session
    sess = verify_session(request.cookies.get(SESSION_COOKIE, ""))
    if sess and sess.get("role") == "admin":
        return True
    # Legacy ADMIN_TOKEN path.
    expected = os.environ.get("ADMIN_TOKEN", "").strip()
    if not expected:
        return False
    provided = (
        request.query_params.get("token", "") or
        request.headers.get("x-admin-token", "") or
        request.cookies.get(ADMIN_COOKIE, "")
    ).strip()
    # Constant-time compare so a network-adjacent attacker can't recover
    # the token byte-by-byte via response timing.
    return provided != "" and hmac.compare_digest(provided, expected)


def _admin_gate(request: Request):
    """Return a 401 JSONResponse when the caller isn't an admin, else
    None. Used to protect state-mutating / cache-clearing API endpoints
    that were previously open to anonymous requests."""
    if not _check_admin_token(request):
        return JSONResponse({"error": "Unauthorized — admin only."},
                            status_code=401)
    return None


def _coerce_float(value):
    """Parse a float from a JSON body value. Returns None when the value
    is missing or non-numeric so callers can reply 400 instead of
    raising a 500."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_redirect(r: str) -> str:
    """Only allow same-site relative paths — never an off-site open redirect."""
    r = (r or "/").strip()
    return r if (r.startswith("/") and not r.startswith("//")) else "/"


def _admin_login_success(request: Request, redirect: str):
    """Set the 30-day admin cookie and bounce to the target page."""
    resp = RedirectResponse(url=_safe_redirect(redirect), status_code=302)
    resp.set_cookie(
        key=ADMIN_COOKIE,
        value=os.environ.get("ADMIN_TOKEN", "").strip(),
        max_age=60 * 60 * 24 * 30,   # 30 days
        httponly=True,
        # secure=True only over HTTPS so dev/test on http://localhost still
        # round-trips the cookie. Production (Railway) is HTTPS → on.
        secure=(request.url.scheme == "https"),
        samesite="lax",
    )
    return resp


def _admin_login_html(error: bool = False, redirect: str = "/") -> str:
    import html as _html
    red = _html.escape(_safe_redirect(redirect), quote=True)
    err = ('<p class="err">That token didn\'t match — try again.</p>'
           if error else "")
    page = """<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Admin sign in - Market Pulse</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&family=Instrument+Serif:ital@1&display=swap" rel="stylesheet">
<style>
  :root{--bg:#f5f4f1;--surface:#fff;--line:#e5e3df;--ink:#1a1917;--ink3:#6b6864;--primary:#5b4de0;--coral-ink:#a02e22;--coral-soft:#fce8e5;}
  *{box-sizing:border-box;margin:0}
  body{background:var(--bg);color:var(--ink);font-family:'Inter',system-ui,-apple-system,sans-serif;min-height:100vh;display:flex;align-items:center;justify-content:center;padding:1.2rem;}
  .card{background:var(--surface);border:1px solid var(--line);border-radius:16px;padding:2rem 1.8rem;max-width:380px;width:100%;box-shadow:0 12px 32px rgb(26 25 23 / .10);}
  .brand{font-family:'Instrument Serif',Georgia,serif;font-style:italic;font-size:1.7rem;color:var(--primary);}
  h1{font-size:1.05rem;margin:.1rem 0 1.1rem;font-weight:700;}
  label{display:block;font-size:.72rem;text-transform:uppercase;letter-spacing:.05em;color:var(--ink3);font-weight:700;margin-bottom:.35rem;}
  input{width:100%;padding:.7rem .8rem;border:1px solid var(--line);border-radius:10px;font-size:1rem;background:#f5f4f1;color:var(--ink);}
  input:focus{outline:none;border-color:var(--primary);box-shadow:0 0 0 3px #f0effe;}
  button{width:100%;margin-top:1rem;padding:.75rem;border:none;border-radius:10px;background:linear-gradient(135deg,#5b4de0,#8962e5);color:#fff;font-weight:700;font-size:.95rem;cursor:pointer;}
  .err{background:var(--coral-soft);color:var(--coral-ink);font-size:.82rem;padding:.5rem .7rem;border-radius:8px;margin-bottom:.9rem;}
  .note{font-size:.72rem;color:var(--ink3);margin-top:.9rem;line-height:1.5;}
</style></head><body>
<form class="card" method="post" action="/admin/login">
  <div class="brand">Market Pulse</div>
  <h1>Admin sign in</h1>
  __ERR__
  <input type="hidden" name="redirect" value="__REDIRECT__">
  <label for="t">Admin token</label>
  <input id="t" name="token" type="password" autocomplete="current-password" autofocus placeholder="your admin token">
  <button type="submit">Sign in</button>
  <p class="note">Stays signed in on this device for 30 days. This is the ADMIN_TOKEN you set in Railway.</p>
</form></body></html>"""
    return page.replace("__ERR__", err).replace("__REDIRECT__", red)


@app.get("/admin/login")
async def admin_login(request: Request, token: str = "", redirect: str = "/"):
    """Admin sign-in. With a valid ?token=<ADMIN_TOKEN> it sets the 30-day
    mp_admin cookie and redirects (keeps old bookmarks working). With no /
    wrong token it now renders a simple sign-in form instead of raw JSON."""
    from fastapi.responses import HTMLResponse
    expected = os.environ.get("ADMIN_TOKEN", "").strip()
    if token and expected and hmac.compare_digest(token, expected):
        return _admin_login_success(request, redirect)
    return HTMLResponse(_admin_login_html(error=bool(token), redirect=redirect),
                        status_code=(401 if token else 200))


@app.post("/admin/login")
async def admin_login_post(request: Request):
    """Form submit from the sign-in page. Token comes in the POST body (not
    the URL), so it never lands in history or logs. Parses urlencoded body
    directly to avoid a multipart dependency."""
    from fastapi.responses import HTMLResponse
    from urllib.parse import parse_qs
    raw = (await request.body()).decode("utf-8", "ignore")
    data = parse_qs(raw, keep_blank_values=True)
    token = (data.get("token", [""])[0]).strip()
    redirect = data.get("redirect", ["/"])[0]
    expected = os.environ.get("ADMIN_TOKEN", "").strip()
    if expected and hmac.compare_digest(token, expected):
        return _admin_login_success(request, redirect)
    return HTMLResponse(_admin_login_html(error=True, redirect=redirect), status_code=401)


def _check_pipeline_access(request: Request) -> bool:
    """Pipeline routes: admin OR sales role. Used for /pipeline/* paths
    so the sales team can sign in via Google and access only that
    section while admins keep full app access."""
    if _check_admin_token(request):
        return True
    from auth import SESSION_COOKIE, verify_session
    sess = verify_session(request.cookies.get(SESSION_COOKIE, ""))
    return bool(sess and sess.get("role") in ("admin", "sales"))


def _current_user(request: Request) -> dict | None:
    """Returns {email, role} from the Google OAuth session, or None.
    Does NOT return for legacy ADMIN_TOKEN sessions (those have no
    email); callers handle that via _check_admin_token."""
    from auth import SESSION_COOKIE, verify_session
    return verify_session(request.cookies.get(SESSION_COOKIE, ""))


def _callback_url(request: Request) -> str:
    """Build the OAuth callback URL, respecting X-Forwarded-Proto so
    Railway's HTTPS terminator doesn't make us hand Google an http://
    URL that won't match the registered redirect URI."""
    scheme = request.headers.get("x-forwarded-proto", request.url.scheme)
    host = request.headers.get("x-forwarded-host") or request.headers.get("host") or request.url.netloc
    return f"{scheme}://{host}/auth/google/callback"


@app.get("/auth/google/login")
async def auth_google_login(request: Request, redirect: str = "/pipeline"):
    """Start the Google OAuth round-trip. Caches the post-login
    redirect target + CSRF state in short-lived cookies."""
    from auth import google_oauth_redirect, new_state, OAUTH_STATE_COOKIE, OAUTH_REDIRECT_COOKIE
    callback = _callback_url(request)
    state = new_state()
    url = google_oauth_redirect(callback, state)
    if not url:
        return JSONResponse(
            {"error": "Google sign-in not configured. Set GOOGLE_CLIENT_ID."},
            status_code=500,
        )
    secure = callback.startswith("https://")
    resp = RedirectResponse(url, status_code=303)
    resp.set_cookie(OAUTH_STATE_COOKIE, state, max_age=600,
                    httponly=True, secure=secure, samesite="lax")
    safe_redirect = redirect if redirect.startswith("/") else "/pipeline"
    resp.set_cookie(OAUTH_REDIRECT_COOKIE, safe_redirect, max_age=600,
                    httponly=True, secure=secure, samesite="lax")
    return resp


@app.get("/auth/google/callback")
async def auth_google_callback(request: Request, code: str = "", state: str = "",
                               error: str = ""):
    """Google OAuth redirect target. Exchanges code → tokens → userinfo,
    validates the email against ADMIN_EMAILS / SALES_EMAILS, then sets
    the mp_session cookie and bounces to the original destination."""
    from auth import (SESSION_COOKIE, OAUTH_STATE_COOKIE, OAUTH_REDIRECT_COOKIE,
                      google_exchange_code, google_fetch_userinfo,
                      role_for_email, make_session)
    if error:
        return JSONResponse({"error": f"Google sign-in cancelled: {error}"},
                            status_code=400)
    expected_state = request.cookies.get(OAUTH_STATE_COOKIE, "")
    if not state or state != expected_state:
        return JSONResponse({"error": "Invalid OAuth state."}, status_code=400)
    callback = _callback_url(request)
    tokens = google_exchange_code(code, callback)
    if not tokens or not tokens.get("access_token"):
        return JSONResponse({"error": "Failed to exchange code for tokens."},
                            status_code=400)
    info = google_fetch_userinfo(tokens["access_token"])
    if not info or not info.get("email"):
        return JSONResponse({"error": "Could not load Google profile."},
                            status_code=400)
    if info.get("verified_email") is False:
        return JSONResponse({"error": "Google email not verified."},
                            status_code=403)
    email = info["email"].strip().lower()
    role = role_for_email(email)
    if not role:
        return JSONResponse(
            {"error": f"{email} is not on the access list. Ask Aaron to add it."},
            status_code=403,
        )
    token = make_session(email, role)
    if not token:
        return JSONResponse({"error": "SESSION_SECRET not set on server."},
                            status_code=500)
    redirect_to = request.cookies.get(OAUTH_REDIRECT_COOKIE, "/pipeline")
    if not redirect_to.startswith("/"):
        redirect_to = "/pipeline"
    secure = request.url.scheme == "https"
    resp = RedirectResponse(redirect_to, status_code=303)
    resp.set_cookie(SESSION_COOKIE, token, max_age=60 * 60 * 24 * 30,
                    httponly=True, secure=secure, samesite="lax")
    resp.delete_cookie(OAUTH_STATE_COOKIE)
    resp.delete_cookie(OAUTH_REDIRECT_COOKIE)
    return resp


@app.get("/auth/logout")
async def auth_logout(request: Request):
    """Sign the user out of both the Google session AND the legacy
    admin cookie."""
    from auth import SESSION_COOKIE
    # /admin/login 401s without a token, so logging out there showed a
    # raw JSON error. Bounce to the friendly sign-in landing instead.
    resp = RedirectResponse("/sign-in", status_code=303)
    resp.delete_cookie(SESSION_COOKIE)
    resp.delete_cookie(ADMIN_COOKIE)
    return resp


@app.get("/admin/logout")
async def admin_logout():
    """Clears the admin cookie. Useful for testing the gated UX."""
    resp = RedirectResponse(url="/", status_code=302)
    resp.delete_cookie(key=ADMIN_COOKIE)
    return resp


# Expose to Jinja so base.html can hide admin-only nav links without
# the route handler having to pass `is_admin` through every render.
templates.env.globals["is_admin"] = _check_admin_token
templates.env.globals["current_user"] = _current_user
templates.env.globals["pipeline_access"] = _check_pipeline_access


@app.get("/sign-in")
async def sign_in_page(request: Request, redirect: str = "/pipeline"):
    """Lightweight sign-in landing — offers Google OAuth if configured,
    otherwise falls back to the legacy admin-token URL."""
    return templates.TemplateResponse("sign_in.html", {
        "request": request,
        "redirect": redirect,
        "google_configured": bool(os.environ.get("GOOGLE_CLIENT_ID", "").strip()),
    })


@app.get("/admin/signups")
async def admin_signups(request: Request, format: str = "json", limit: int = 500):
    """Admin-gated signup list. Format: 'json' (default) or 'csv'.
    Hit with ?token=<your-ADMIN_TOKEN> or X-Admin-Token header."""
    if not _check_admin_token(request):
        return JSONResponse(
            {"error": "Unauthorized — pass ?token=<ADMIN_TOKEN> or X-Admin-Token header."},
            status_code=401,
        )
    limit = max(1, min(int(limit), 5000))
    rows = list_users(limit=limit)
    total = get_user_count()
    if format.lower() == "csv":
        # Tiny inline CSV — avoids importing a heavy dep for a 5-col file.
        import io, csv
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(["id", "email", "name", "source", "created_at", "user_agent"])
        for r in rows:
            w.writerow([r["id"], r["email"], r["name"] or "", r["source"] or "",
                        r["created_at"] or "", r["user_agent"] or ""])
        return Response(
            content=buf.getvalue(),
            media_type="text/csv",
            headers={"Content-Disposition": 'attachment; filename="signups.csv"'},
        )
    return JSONResponse({"total": total, "limit": limit, "users": rows})



# ─── National ZIPs viewport endpoint (Phase 2 of national rollout) ──
# Backed by data/zips.db, built monthly by scripts/build_national_zips.py.
# Phase 3 (Leaflet integration) calls this on `moveend` with the
# current viewport's bbox to fetch only the ZIPs that need to render.

ZIPS_DB_PATH = Path(__file__).parent / "data" / "zips.db"

# Whitelist of persona → DB column. Looked up by string so the ORDER BY
# slot can be safely interpolated (the value is never user-supplied).
PERSONA_COLUMNS = {
    "balanced":  "composite_balanced",
    "investor":  "composite_investor",
    "lifestyle": "composite_lifestyle",
}


def _open_zips_db() -> sqlite3.Connection | None:
    """Returns a read-only SQLite connection or None if the DB hasn't
    been built yet. Per-request connections — SQLite open is sub-ms,
    not worth pooling. Read-only `mode=ro` URI prevents accidental
    writes from the request handler path."""
    if not ZIPS_DB_PATH.exists():
        return None
    uri = f"file:{ZIPS_DB_PATH}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


@app.get("/api/zips")
async def api_zips(
    lat1: float, lng1: float, lat2: float, lng2: float,
    persona: str = "balanced",
    limit: int = 500,
):
    """Top-N ZIPs within a bbox, ranked by persona composite.

    Query params:
      lat1,lng1,lat2,lng2 — opposite corners of the viewport (any order).
      persona             — one of: balanced (default) | investor | lifestyle.
      limit               — 1-2000, default 500. Caps protect the JSON payload size.
    """
    persona_col = PERSONA_COLUMNS.get(persona, "composite_balanced")
    limit = max(1, min(int(limit), 2000))
    conn = _open_zips_db()
    if conn is None:
        # DB hasn't been built yet — return empty + a clear meta flag
        # so the frontend can surface "national data not yet loaded"
        # instead of mis-rendering an empty map.
        return JSONResponse({
            "zips": [],
            "meta": {
                "count": 0, "limit": limit, "persona": persona,
                "db_missing": True,
                "message": "Run the refresh-national-zips workflow to populate data/zips.db.",
            },
        })
    # county + neighborhood are added in the v2 schema (P131). For
    # backwards-compat with a deployed zips.db that pre-dates the new
    # columns, peek at table_info first and substitute NULL placeholders
    # if either column is missing. After the next refresh-national-zips
    # run rebuilds the DB, this branch goes unused.
    try:
        existing_cols = {r["name"] for r in conn.execute("PRAGMA table_info(zips)").fetchall()}
        county_expr = "county" if "county" in existing_cols else "NULL AS county"
        nbhd_expr = "neighborhood" if "neighborhood" in existing_cols else "NULL AS neighborhood"
        # Phase-A forecast columns (P142). Backwards-compat with old
        # zips.db via NULL-AS shims, same pattern as county/neighborhood.
        f_val_expr = "forecast_home_value_12mo" if "forecast_home_value_12mo" in existing_cols else "NULL AS forecast_home_value_12mo"
        f_pct_expr = "forecast_pct_change_12mo" if "forecast_pct_change_12mo" in existing_cols else "NULL AS forecast_pct_change_12mo"
        rows = conn.execute(
            f"""
            SELECT zip, state, name, {county_expr}, {nbhd_expr}, lat, lng,
                   median_home_value, home_value_yoy,
                   median_rent_monthly, cap_rate_pct,
                   median_household_income, pct_bachelors,
                   population, walk_score, crime_index, restaurant_score,
                   {f_val_expr}, {f_pct_expr},
                   {persona_col} AS composite, rent_source, as_of
            FROM zips
            WHERE lat BETWEEN ? AND ?
              AND lng BETWEEN ? AND ?
            ORDER BY {persona_col} DESC
            LIMIT ?
            """,
            (
                min(lat1, lat2), max(lat1, lat2),
                min(lng1, lng2), max(lng1, lng2),
                limit,
            ),
        ).fetchall()
    finally:
        conn.close()
    # Pre-compute qualifying_income per ZIP — annual gross income needed
    # to buy a home at this ZIP's median value. Same NAR/HSH methodology
    # as /affordability and the metro popup: 20% down, 30Y at the current
    # rate, 28% front-end DTI on full PITI (P&I + state tax + state ins).
    zips = [
        {
            "zip": r["zip"],
            "state": r["state"],
            "name": r["name"],
            "county": r["county"] or None,
            "neighborhood": r["neighborhood"] or None,
            "lat": r["lat"],
            "lng": r["lng"],
            "home_value": r["median_home_value"],
            "home_value_yoy": r["home_value_yoy"],
            "forecast_12mo": r["forecast_home_value_12mo"],
            "forecast_pct_12mo": r["forecast_pct_change_12mo"],
            "rent": r["median_rent_monthly"],
            "cap_rate_pct": r["cap_rate_pct"],
            "income": r["median_household_income"],
            "pct_bachelors": r["pct_bachelors"],
            "population": r["population"],
            "walk_score": r["walk_score"],
            "crime_index": r["crime_index"],
            "restaurant_score": r["restaurant_score"],
            "composite": round(r["composite"], 1) if r["composite"] is not None else None,
            "is_imputed": r["rent_source"] == "imputed",
            "qualifying_income": qualifying_income(r["median_home_value"], r["state"], MORTGAGE_30Y_RATE),
        }
        for r in rows
    ]
    as_of = rows[0]["as_of"] if rows else None
    return JSONResponse({
        "zips": zips,
        "meta": {
            "count": len(zips),
            "limit": limit,
            "persona": persona,
            "bbox": [lat1, lng1, lat2, lng2],
            "as_of": as_of,
            "db_missing": False,
        },
    })


@app.get("/api/zips/stats")
async def api_zips_stats():
    """Health/monitoring endpoint for the national ZIPs DB. Cheap to
    hit; useful for dashboards and for catching feed regressions
    after the monthly refresh."""
    conn = _open_zips_db()
    if conn is None:
        return JSONResponse({"db_missing": True}, status_code=503)
    try:
        total = conn.execute("SELECT COUNT(*) FROM zips").fetchone()[0]
        as_of = conn.execute("SELECT MAX(as_of) FROM zips").fetchone()[0]
        states = conn.execute(
            "SELECT state, COUNT(*) AS n FROM zips WHERE state != '' GROUP BY state ORDER BY n DESC"
        ).fetchall()
        rent_sources = conn.execute(
            "SELECT rent_source, COUNT(*) AS n FROM zips GROUP BY rent_source"
        ).fetchall()
    finally:
        conn.close()
    return JSONResponse({
        "db_missing": False,
        "total_zips": total,
        "as_of": as_of,
        "states": {r["state"]: r["n"] for r in states},
        "rent_sources": {r["rent_source"]: r["n"] for r in rent_sources},
    })


@app.get("/api/search")
async def api_search(q: str = "", limit: int = 8):
    """Free-text search across ZIPs, metros, and states. Used by the
    /map floating search bar. Returns categorized results in priority
    order (exact-zip > neighborhood > city > metro > state). Empty
    query returns nothing — no implicit 'show everything' since that
    would be a 30K-row dump."""
    q = (q or "").strip()
    if len(q) < 2:
        return JSONResponse({"results": [], "query": q})
    limit = max(1, min(int(limit), 15))

    results: list[dict] = []
    qlike = f"%{q}%"

    # ─── ZIPs from zips.db ─────────────────────────────────────────
    # Backwards-compat with old zips.db that pre-dates county +
    # neighborhood (P131 schema). PRAGMA the columns and substitute
    # empty strings for missing ones.
    from data_providers import CHOROPLETH_STATES as _CP_STATES
    conn = _open_zips_db()
    if conn is not None:
        try:
            existing = {r["name"] for r in conn.execute("PRAGMA table_info(zips)").fetchall()}
            nbhd_col = "neighborhood" if "neighborhood" in existing else "''"
            cnty_col = "county" if "county" in existing else "''"
            select_cols = (
                f"zip, state, name, lat, lng, "
                f"{cnty_col} AS county, {nbhd_col} AS neighborhood"
            )

            # Exact ZIP match first — instant top result.
            seen_zips: set[str] = set()
            if q.isdigit() and len(q) == 5:
                row = conn.execute(
                    f"SELECT {select_cols} FROM zips WHERE zip = ?", (q,)
                ).fetchone()
                if row:
                    results.append({
                        "type": "zip", "zip": row["zip"], "state": row["state"],
                        "name": row["name"],
                        "neighborhood": row["neighborhood"] or None,
                        "county": row["county"] or None,
                        "lat": row["lat"], "lng": row["lng"],
                    })
                    seen_zips.add(row["zip"])

            # Fuzzy match across neighborhood, name, county. Order by
            # match-priority via UNION: neighborhood hits first, then
            # name (city), then county. Per-clause LIMIT keeps each
            # bucket bounded.
            where_clauses = ["name LIKE ? COLLATE NOCASE"]
            params = [qlike]
            if "neighborhood" in existing:
                where_clauses.insert(0, "neighborhood LIKE ? COLLATE NOCASE")
                params.insert(0, qlike)
            if "county" in existing:
                where_clauses.append("county LIKE ? COLLATE NOCASE")
                params.append(qlike)
            where = " OR ".join(where_clauses)
            sql = f"SELECT {select_cols} FROM zips WHERE {where} LIMIT ?"
            params.append(limit * 2)   # over-fetch for de-dup against exact-match

            for row in conn.execute(sql, params).fetchall():
                if row["zip"] in seen_zips:
                    continue
                seen_zips.add(row["zip"])
                results.append({
                    "type": "zip", "zip": row["zip"], "state": row["state"],
                    "name": row["name"],
                    "neighborhood": row["neighborhood"] or None,
                    "county": row["county"] or None,
                    "lat": row["lat"], "lng": row["lng"],
                })
                if len(results) >= limit:
                    break
        finally:
            conn.close()

    # ─── Metros (in-memory, fast — only 112 of them) ───────────────
    qlower = q.lower()
    metros: list[dict] = []
    for slug, cfg in STATE_METROS.items():
        if qlower in cfg["metro_label"].lower() or qlower == cfg["state"].lower():
            metros.append({
                "type": "metro", "slug": slug, "state": cfg["state"],
                "label": cfg["metro_label"],
                "lat": cfg["map_center"]["lat"], "lng": cfg["map_center"]["lng"],
            })
    metros.sort(key=lambda m: m["label"])

    # ─── States (in-memory, fast — 51 of them) ─────────────────────
    states: list[dict] = []
    for code, sd in _CP_STATES.items():
        name = sd.get("name", "")
        if qlower in name.lower() or qlower == code.lower():
            states.append({"type": "state", "code": code, "name": name})
    states.sort(key=lambda s: s["name"])

    return JSONResponse({
        "results": results + metros[:5] + states[:5],
        "query": q,
    })


# ─── /zip/{zip} detail page (Phase A.1) ────────────────────────────
# Server-rendered ZIP detail page with multi-horizon forecast,
# historical chart, and county/state comparison strip. Linked from
# the popup's "View full report →" button.

@app.get("/zip/{zip}")
async def zip_detail(request: Request, zip: str):
    zip = zip.strip()
    conn = _open_zips_db()
    if conn is None:
        return RedirectResponse(url="/map", status_code=302)
    try:
        # Detect schema version once; new columns are optional so older
        # zips.db (pre-P143) still renders the page (with chart hidden).
        existing = {r["name"] for r in conn.execute("PRAGMA table_info(zips)").fetchall()}
        # Pull the full row for this ZIP, plus the county + state aggregates
        # for the comparison strip. SELECT * because most fields go straight
        # to the template and listing them all is noisy.
        row = conn.execute("SELECT * FROM zips WHERE zip = ?", (zip,)).fetchone()
        if not row:
            conn.close()
            return RedirectResponse(url="/map", status_code=302)

        # Aggregates (median across the relevant pool). Median is more
        # robust than mean against single-ZIP outliers like Beverly Hills.
        def _median_for(where_clause: str, params: tuple) -> dict:
            agg = conn.execute(
                f"""SELECT
                    median_home_value, home_value_yoy, cap_rate_pct,
                    median_household_income
                FROM zips WHERE {where_clause} ORDER BY zip""",
                params,
            ).fetchall()
            if not agg:
                return {}
            def _med(key):
                vals = [r[key] for r in agg if r[key] is not None]
                if not vals:
                    return None
                vals.sort()
                n = len(vals)
                return vals[n // 2] if n % 2 else (vals[n // 2 - 1] + vals[n // 2]) / 2
            return {
                "n": len(agg),
                "median_home_value": _med("median_home_value"),
                "home_value_yoy": _med("home_value_yoy"),
                "cap_rate_pct": _med("cap_rate_pct"),
                "median_household_income": _med("median_household_income"),
            }

        county_agg = _median_for(
            "county = ? AND state = ?",
            (row["county"] if "county" in existing else "", row["state"]),
        ) if (row["county"] if "county" in existing else "") else {}
        state_agg = _median_for("state = ?", (row["state"],))
    finally:
        conn.close()

    # Decode history JSON for the chart. Empty list when missing — the
    # template hides the chart in that case.
    import json as _json
    history_values = []
    try:
        if "history_zhvi" in existing and row["history_zhvi"]:
            history_values = _json.loads(row["history_zhvi"]) or []
    except (ValueError, TypeError):
        history_values = []

    # Build the forecast trajectory the chart uses for the band: linear
    # interpolation between the four horizons (3/6/12/60 months).
    # Honest about the model's coarseness — we only forecast at those
    # four points, not every month — but it visualizes the trend.
    forecast_points: list[dict] = []
    if "forecast_60mo_value" in existing and row["forecast_60mo_value"]:
        last = history_values[-1] if history_values else (row["median_home_value"] or 0)
        for h, v in [
            (3,  row["forecast_3mo_value"] if "forecast_3mo_value" in existing else None),
            (6,  row["forecast_6mo_value"] if "forecast_6mo_value" in existing else None),
            (12, row["forecast_home_value_12mo"] if "forecast_home_value_12mo" in existing else None),
            (60, row["forecast_60mo_value"] if "forecast_60mo_value" in existing else None),
        ]:
            if v is not None:
                forecast_points.append({"h": h, "value": v})

    return templates.TemplateResponse("zip_detail.html", {
        "request": request,
        "zip": dict(row),
        "history_values": history_values,
        "history_as_of": row["as_of"] if "as_of" in row.keys() else "",
        "forecast_points": forecast_points,
        "county_agg": county_agg,
        "state_agg": state_agg,
        "schema_has": {
            "forecast": "forecast_60mo_value" in existing,
            "history": "history_zhvi" in existing,
            "neighborhood": "neighborhood" in existing,
            "county": "county" in existing,
        },
    })


@app.get("/api/finance/screener")
async def api_screener():
    """Net-net / deep value screener powered by SEC EDGAR."""
    # build_net_net_screener() fans out to SEC EDGAR on a cache miss —
    # keep it off the event loop.
    data = await asyncio.to_thread(build_net_net_screener)
    return JSONResponse(data)

@app.get("/api/finance/rules")
async def api_rules():
    """Return the current screening rules."""
    from sec_edgar import SCREENER_RULES
    return JSONResponse(SCREENER_RULES)


@app.get("/api/finance/refresh")
async def api_refresh(request: Request):
    # Admin-only: this clears the cache and triggers a live SEC EDGAR
    # rebuild, which is exactly what we don't want anonymous callers
    # hammering.
    gate = _admin_gate(request)
    if gate:
        return gate
    from pathlib import Path
    for f in ["net_net_screener.json", "sec_financials.json"]:
        p = Path(f"/tmp/market_pulse_cache/{f}")
        if p.exists():
            p.unlink()
    data = await asyncio.to_thread(build_net_net_screener)
    count = len(data) if isinstance(data, list) else 0
    net_nets = sum(1 for d in data if isinstance(d, dict) and d.get("is_net_net")) if isinstance(data, list) else 0
    return JSONResponse({"count": count, "net_nets": net_nets, "status": "refreshed"})


# ── Monthly screener snapshots ──
# A GitHub Action runs scripts/refresh_screener.py on the 1st of
# each month and commits data/screener_snapshots/YYYY-MM.json. These
# two endpoints expose the historical archive to /finance so users
# can browse what net-nets looked like in any past month.
_SNAPSHOT_DIR = Path(__file__).resolve().parent / "data" / "screener_snapshots"


@app.get("/api/finance/snapshots")
async def api_snapshot_list():
    """List available monthly snapshots, newest first.

    Returns: { "months": ["2026-05", "2026-04", ...], "latest": "2026-05" }.
    """
    if not _SNAPSHOT_DIR.exists():
        return JSONResponse({"months": [], "latest": None})
    months = sorted(
        (p.stem for p in _SNAPSHOT_DIR.glob("*.json")),
        reverse=True,
    )
    return JSONResponse({"months": months, "latest": months[0] if months else None})


@app.get("/api/finance/snapshot/{month}")
async def api_snapshot(month: str):
    """Return the full snapshot payload for a given YYYY-MM.

    Format mirrors what refresh_screener.py wrote:
        { "_meta": {...}, "net_nets": [...] }
    """
    # Defensive: only allow the strict format so a path-traversal
    # request like /api/finance/snapshot/..%2Fetc%2Fpasswd is rejected
    # before we touch the filesystem.
    if len(month) != 7 or month[4] != "-" or not (month[:4].isdigit() and month[5:].isdigit()):
        return JSONResponse({"error": "month must be YYYY-MM"}, status_code=400)
    path = _SNAPSHOT_DIR / f"{month}.json"
    if not path.exists():
        return JSONResponse({"error": f"no snapshot for {month}"}, status_code=404)
    try:
        return JSONResponse(json.loads(path.read_text()))
    except Exception as e:
        return JSONResponse({"error": f"failed to read snapshot: {e}"}, status_code=500)


# ── Lynch GARP snapshots ──
# Sibling endpoints to /api/finance/snapshot* — same shape, different
# folder. Powers the /lynch page's month dropdown.
_LYNCH_SNAPSHOT_DIR = Path(__file__).resolve().parent / "data" / "lynch_snapshots"


@app.get("/api/lynch/snapshots")
async def api_lynch_snapshot_list():
    if not _LYNCH_SNAPSHOT_DIR.exists():
        return JSONResponse({"months": [], "latest": None})
    months = sorted(
        (p.stem for p in _LYNCH_SNAPSHOT_DIR.glob("*.json")),
        reverse=True,
    )
    return JSONResponse({"months": months, "latest": months[0] if months else None})


@app.get("/api/lynch/snapshot/{month}")
async def api_lynch_snapshot(month: str):
    if len(month) != 7 or month[4] != "-" or not (month[:4].isdigit() and month[5:].isdigit()):
        return JSONResponse({"error": "month must be YYYY-MM"}, status_code=400)
    path = _LYNCH_SNAPSHOT_DIR / f"{month}.json"
    if not path.exists():
        return JSONResponse({"error": f"no snapshot for {month}"}, status_code=404)
    try:
        return JSONResponse(json.loads(path.read_text()))
    except Exception as e:
        return JSONResponse({"error": f"failed to read snapshot: {e}"}, status_code=500)




# ═══════════════════════════════════════════════════
# PRICE DATABASE ENDPOINTS
# ═══════════════════════════════════════════════════

@app.get("/api/prices")
async def api_get_prices():
    """Get all saved user prices from Postgres."""
    return JSONResponse(get_all_prices())


@app.post("/api/prices")
async def api_save_price(request: Request):
    """Save a single price. Body: {ticker, price, notes?}"""
    gate = _admin_gate(request)
    if gate:
        return gate
    body = await request.json()
    ticker = body.get("ticker", "").upper()
    price = _coerce_float(body.get("price"))
    notes = body.get("notes", "")
    if not ticker or price is None:
        return JSONResponse({"error": "ticker and numeric price required"}, status_code=400)
    ok = save_price(ticker, price, notes)
    return JSONResponse({"ok": ok, "ticker": ticker, "price": price})


@app.post("/api/prices/bulk")
async def api_save_bulk(request: Request):
    """Save multiple prices. Body: {prices: {TICKER: price, ...}}"""
    gate = _admin_gate(request)
    if gate:
        return gate
    body = await request.json()
    raw = body.get("prices", {})
    # Keep only entries that parse to a real number so one bad value
    # can't 500 the whole bulk save.
    prices = {}
    if isinstance(raw, dict):
        for tkr, val in raw.items():
            fv = _coerce_float(val)
            if fv is not None:
                prices[str(tkr).upper()] = fv
    ok = save_prices_bulk(prices)
    return JSONResponse({"ok": ok, "count": len(prices)})


@app.delete("/api/prices/{ticker}")
async def api_delete_price(ticker: str, request: Request):
    """Delete a saved price."""
    gate = _admin_gate(request)
    if gate:
        return gate
    ok = delete_price(ticker.upper())
    return JSONResponse({"ok": ok})


@app.get("/results")
async def results_page(request: Request):
    # Admin-only — same gating pattern as /finance.
    if not _check_admin_token(request):
        return RedirectResponse(url="/map", status_code=302)
    return templates.TemplateResponse("results.html", {"request": request})


# ═══════════════════════════════════════════════════
# PAPER PORTFOLIO ENDPOINTS
# ═══════════════════════════════════════════════════

@app.get("/api/portfolios")
async def api_get_portfolios():
    """Get all portfolio snapshots with holdings and updates."""
    return JSONResponse(get_all_portfolios())


@app.post("/api/portfolios/lock")
async def api_lock_portfolio(request: Request):
    """Lock in a new quarterly portfolio. Body: {name, holdings: [{ticker, entry_price, ...}], iwm_price}"""
    gate = _admin_gate(request)
    if gate:
        return gate
    body = await request.json()
    name = body.get("name", "")
    holdings = body.get("holdings", [])
    iwm = _coerce_float(body.get("iwm_price", 0))
    if not name or not holdings:
        return JSONResponse({"error": "name and holdings required"}, status_code=400)
    if iwm is None:
        return JSONResponse({"error": "iwm_price must be numeric"}, status_code=400)
    result = lock_portfolio(name, holdings, iwm)
    return JSONResponse(result)


@app.post("/api/portfolios/update")
async def api_update_portfolio(request: Request):
    """Monthly price update. Body: {name, prices: {ticker: price}, iwm_price}"""
    gate = _admin_gate(request)
    if gate:
        return gate
    body = await request.json()
    name = body.get("name", "")
    prices = body.get("prices", {})
    iwm = _coerce_float(body.get("iwm_price", 0))
    if not name or not prices:
        return JSONResponse({"error": "name and prices required"}, status_code=400)
    if iwm is None:
        return JSONResponse({"error": "iwm_price must be numeric"}, status_code=400)
    result = update_portfolio_prices(name, prices, iwm)
    return JSONResponse(result)


@app.post("/api/portfolios/exit")
async def api_exit_holding(request: Request):
    """Exit a single holding. Body: {portfolio_name, ticker, exit_price, reason}"""
    gate = _admin_gate(request)
    if gate:
        return gate
    body = await request.json()
    exit_price = _coerce_float(body.get("exit_price", 0))
    if exit_price is None:
        return JSONResponse({"error": "exit_price must be numeric"}, status_code=400)
    ok = exit_holding(body.get("portfolio_name"), body.get("ticker"),
                      exit_price, body.get("reason", "held to maturity"))
    return JSONResponse({"ok": ok})


@app.post("/api/portfolios/close")
async def api_close_portfolio(request: Request):
    """Close a portfolio after 12 months. Body: {name, iwm_exit_price}"""
    gate = _admin_gate(request)
    if gate:
        return gate
    body = await request.json()
    iwm_exit = _coerce_float(body.get("iwm_exit_price", 0))
    if iwm_exit is None:
        return JSONResponse({"error": "iwm_exit_price must be numeric"}, status_code=400)
    result = close_portfolio(body.get("name"), iwm_exit)
    return JSONResponse(result)


@app.get("/api/refresh-all")
async def api_refresh_all(request: Request):
    """Clear all SEC EDGAR caches and force re-fetch. Admin-only — a
    live re-fetch hammers SEC EDGAR, so this must not be anonymous."""
    gate = _admin_gate(request)
    if gate:
        return gate
    from pathlib import Path
    cache_dir = Path("/tmp/market_pulse_cache")
    cleared = 0
    for f in cache_dir.glob("*.json"):
        try:
            f.unlink()
            cleared += 1
        except OSError:
            pass
    return JSONResponse({"cleared": cleared, "status": "All caches cleared. Reload the page to fetch fresh data."})

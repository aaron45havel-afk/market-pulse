"""Market Pulse — Real Estate & Finance Dashboard."""
import os, logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import JSONResponse, RedirectResponse
from dotenv import load_dotenv
from data_providers import (
    get_all_state_data, get_county_data, get_national_data, STATES, COUNTIES
)
from sec_edgar import build_net_net_screener
from state_neighborhoods import (
    get_state_neighborhoods, list_supported_states, default_metro_slug,
    STATE_METROS, STATE_TO_METROS, metros_for_state,
)
from database import (init_db, save_price, save_prices_bulk, get_all_prices, delete_price,
                      lock_portfolio, update_portfolio_prices, exit_holding,
                      close_portfolio, get_all_portfolios)

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FRED_API_KEY = os.getenv("FRED_API_KEY")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Market Pulse starting up...")
    init_db()
    yield
    logger.info("Market Pulse shutting down.")


app = FastAPI(title="Market Pulse", lifespan=lifespan)
static_dir = os.path.join(os.path.dirname(__file__) or ".", "static")
if os.path.isdir(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")
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
    )
    metros = []
    for slug, cfg in STATE_METROS.items():
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
        # State-level lookups for tax-haven + growth-momentum signals.
        sunshine = STATE_SUNSHINE_DAYS.get(cfg["state"], 200)
        state_income_tax = STATE_INCOME_TAX_EFFECTIVE.get(cfg["state"], 0.045)
        state_pop_growth = STATE_POPULATION_GROWTH.get(cfg["state"], 0.5)
        metros.append({
            "slug": slug,
            "state": cfg["state"],
            "metro_label": cfg["metro_label"],
            "map_center": cfg["map_center"],
            "top_zip": {
                "zip": top_zip["zip"],
                "name": top_zip["name"],
                "composite_score": top_zip["composite_score"],
                "cap_rate_pct": top_zip["cap_rate_pct"],
            },
            "zip_count": n,
            "avg_composite": round(avg_score, 1),
            "composite_by_persona": composite_by_persona,
            "avg_cap_rate_pct": round(avg_cap, 2),
            "avg_home_value": round(avg_home),
            "avg_rent": round(avg_rent),
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
    # We also stamp `has_metros: True` for the 8 states with metro
    # coverage so the choropleth can highlight them differently.
    states_with_metros = {cfg["state"] for cfg in STATE_METROS.values()}
    choropleth_by_fips = {}
    for code, sd in CHOROPLETH_STATES.items():
        choropleth_by_fips[sd["fips"]] = {
            "code": code,
            "name": sd["name"],
            "income_tax": sd["income_tax"],
            "property_tax": sd["property_tax"],
            "sunshine": sd["sunshine"],
            "pop_growth": sd["pop_growth"],
            "col": sd["col"],
            "has_metros": code in states_with_metros,
        }
    return templates.TemplateResponse("national_map.html", {
        "request": request,
        "metros": metros,
        "choropleth_states": choropleth_by_fips,
        "choropleth_metrics": CHOROPLETH_METRICS,
    })


@app.get("/real-estate")
async def real_estate(request: Request):
    # state code → default metro slug, used by the dashboard link to route
    # each state tab at its primary metro (e.g. UT → UT for Provo).
    state_default_metro = {
        s: default_metro_slug(s) for s in list_supported_states()
    }
    return templates.TemplateResponse("real_estate.html", {
        "request": request,
        "states": STATES,
        "counties": COUNTIES,
        "states_with_map": list_supported_states(),
        "state_default_metro": state_default_metro,
    })


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


@app.get("/affordability")
async def affordability(request: Request):
    return templates.TemplateResponse("affordability.html", {"request": request})


@app.get("/api/real-estate")
async def api_real_estate():
    data = get_all_state_data(FRED_API_KEY)
    return JSONResponse(data)


@app.get("/api/real-estate/county/{state}/{fips}")
async def api_county(state: str, fips: str):
    data = get_county_data(FRED_API_KEY, state.upper(), fips)
    return JSONResponse(data)


@app.get("/api/real-estate/national")
async def api_national():
    data = get_national_data(FRED_API_KEY)
    return JSONResponse(data)


@app.get("/api/counties/{state}")
async def api_counties(state: str):
    state = state.upper()
    return JSONResponse(COUNTIES.get(state, {}))


@app.get("/finance")
async def finance(request: Request):
    return templates.TemplateResponse("finance.html", {"request": request})


@app.get("/api/finance/screener")
async def api_screener():
    """Net-net / deep value screener powered by SEC EDGAR."""
    data = build_net_net_screener()
    return JSONResponse(data)

@app.get("/api/finance/rules")
async def api_rules():
    """Return the current screening rules."""
    from sec_edgar import SCREENER_RULES
    return JSONResponse(SCREENER_RULES)


@app.get("/api/finance/refresh")
async def api_refresh():
    from pathlib import Path
    for f in ["net_net_screener.json", "sec_financials.json"]:
        p = Path(f"/tmp/market_pulse_cache/{f}")
        if p.exists():
            p.unlink()
    data = build_net_net_screener()
    count = len(data) if isinstance(data, list) else 0
    net_nets = sum(1 for d in data if isinstance(d, dict) and d.get("is_net_net")) if isinstance(data, list) else 0
    return JSONResponse({"count": count, "net_nets": net_nets, "status": "refreshed"})




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
    body = await request.json()
    ticker = body.get("ticker", "").upper()
    price = body.get("price")
    notes = body.get("notes", "")
    if not ticker or not price:
        return JSONResponse({"error": "ticker and price required"}, status_code=400)
    ok = save_price(ticker, float(price), notes)
    return JSONResponse({"ok": ok, "ticker": ticker, "price": price})


@app.post("/api/prices/bulk")
async def api_save_bulk(request: Request):
    """Save multiple prices. Body: {prices: {TICKER: price, ...}}"""
    body = await request.json()
    prices = body.get("prices", {})
    ok = save_prices_bulk(prices)
    return JSONResponse({"ok": ok, "count": len(prices)})


@app.delete("/api/prices/{ticker}")
async def api_delete_price(ticker: str):
    """Delete a saved price."""
    ok = delete_price(ticker.upper())
    return JSONResponse({"ok": ok})


@app.get("/results")
async def results_page(request: Request):
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
    body = await request.json()
    name = body.get("name", "")
    holdings = body.get("holdings", [])
    iwm = body.get("iwm_price", 0)
    if not name or not holdings:
        return JSONResponse({"error": "name and holdings required"}, status_code=400)
    result = lock_portfolio(name, holdings, float(iwm))
    return JSONResponse(result)


@app.post("/api/portfolios/update")
async def api_update_portfolio(request: Request):
    """Monthly price update. Body: {name, prices: {ticker: price}, iwm_price}"""
    body = await request.json()
    name = body.get("name", "")
    prices = body.get("prices", {})
    iwm = body.get("iwm_price", 0)
    if not name or not prices:
        return JSONResponse({"error": "name and prices required"}, status_code=400)
    result = update_portfolio_prices(name, prices, float(iwm))
    return JSONResponse(result)


@app.post("/api/portfolios/exit")
async def api_exit_holding(request: Request):
    """Exit a single holding. Body: {portfolio_name, ticker, exit_price, reason}"""
    body = await request.json()
    ok = exit_holding(body.get("portfolio_name"), body.get("ticker"),
                      float(body.get("exit_price", 0)), body.get("reason", "held to maturity"))
    return JSONResponse({"ok": ok})


@app.post("/api/portfolios/close")
async def api_close_portfolio(request: Request):
    """Close a portfolio after 12 months. Body: {name, iwm_exit_price}"""
    body = await request.json()
    result = close_portfolio(body.get("name"), float(body.get("iwm_exit_price", 0)))
    return JSONResponse(result)


@app.get("/api/refresh-all")
async def api_refresh_all():
    """Clear all SEC EDGAR caches and force re-fetch."""
    from pathlib import Path
    cache_dir = Path("/tmp/market_pulse_cache")
    cleared = 0
    for f in cache_dir.glob("*.json"):
        try:
            f.unlink()
            cleared += 1
        except:
            pass
    return JSONResponse({"cleared": cleared, "status": "All caches cleared. Reload the page to fetch fresh data."})

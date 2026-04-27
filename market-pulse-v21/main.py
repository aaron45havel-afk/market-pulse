"""Market Pulse — Real Estate & Finance Dashboard."""
import os, logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
from data_providers import (
    get_all_state_data, get_county_data, get_national_data, STATES, COUNTIES
)
from sec_edgar import build_net_net_screener
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
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/real-estate")
async def real_estate(request: Request):
    return templates.TemplateResponse("real_estate.html", {
        "request": request,
        "states": STATES,
        "counties": COUNTIES,
    })


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

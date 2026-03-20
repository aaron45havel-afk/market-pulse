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
from sec_edgar import build_net_net_screener, build_portfolio
from database import init_db, save_price, save_prices_bulk, get_all_prices, delete_price

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


@app.get("/portfolio")
async def portfolio_page(request: Request):
    return templates.TemplateResponse("portfolio.html", {"request": request})


@app.get("/api/finance/portfolio")
async def api_portfolio(capital: float = 50000, positions: int = 25):
    """Build a net-net portfolio with position sizing."""
    data = build_portfolio(capital=capital, num_positions=positions)
    return JSONResponse(data)


@app.get("/api/finance/portfolio/refresh")
async def api_portfolio_refresh():
    from pathlib import Path
    for f in ["portfolio.json", "net_net_screener.json", "sec_financials.json"]:
        p = Path(f"/tmp/market_pulse_cache/{f}")
        if p.exists():
            p.unlink()
    data = build_portfolio()
    return JSONResponse({"positions": len(data.get("positions", [])), "status": "refreshed"})


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

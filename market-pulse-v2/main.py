"""Market Pulse — Real Estate & Finance Dashboard."""
import os, logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
from data_providers import (
    get_all_state_data, get_county_data, get_national_data,
    get_stock_screener_fmp, STATES, COUNTIES
)

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FRED_API_KEY = os.getenv("FRED_API_KEY")
FMP_API_KEY = os.getenv("FMP_API_KEY")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Market Pulse starting up...")
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
    """State-level overview + national indicators."""
    data = get_all_state_data(FRED_API_KEY)
    return JSONResponse(data)


@app.get("/api/real-estate/county/{state}/{fips}")
async def api_county(state: str, fips: str):
    """County-level deep dive — all available FRED series."""
    data = get_county_data(FRED_API_KEY, state.upper(), fips)
    return JSONResponse(data)


@app.get("/api/real-estate/national")
async def api_national():
    """National macro indicators only."""
    data = get_national_data(FRED_API_KEY)
    return JSONResponse(data)


@app.get("/api/counties/{state}")
async def api_counties(state: str):
    """List available counties for a state."""
    state = state.upper()
    return JSONResponse(COUNTIES.get(state, {}))


@app.get("/finance")
async def finance(request: Request):
    return templates.TemplateResponse("finance.html", {"request": request})


@app.get("/api/finance/screener")
async def api_screener():
    data = get_stock_screener_fmp(FMP_API_KEY)
    return JSONResponse(data)


@app.get("/api/finance/refresh")
async def api_refresh():
    from pathlib import Path
    cache_file = Path("/tmp/market_pulse_cache/fmp_screener.json")
    if cache_file.exists():
        cache_file.unlink()
    data = get_stock_screener_fmp(FMP_API_KEY)
    return JSONResponse({"count": len(data) if isinstance(data, list) else 0, "status": "refreshed"})

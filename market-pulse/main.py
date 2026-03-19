"""Market Pulse — Real Estate & Finance Dashboard."""
import os, logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
from data_providers import get_fred_data, get_stock_screener_data, STATES

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FRED_API_KEY = os.getenv("FRED_API_KEY")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Market Pulse starting up...")
    yield
    logger.info("Market Pulse shutting down.")


app = FastAPI(title="Market Pulse", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


@app.get("/")
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/real-estate")
async def real_estate(request: Request):
    return templates.TemplateResponse("real_estate.html", {
        "request": request,
        "states": STATES,
    })


@app.get("/api/real-estate")
async def api_real_estate():
    data = get_fred_data(FRED_API_KEY)
    return JSONResponse(data)


@app.get("/finance")
async def finance(request: Request):
    return templates.TemplateResponse("finance.html", {"request": request})


@app.get("/api/finance/screener")
async def api_screener():
    data = get_stock_screener_data()
    return JSONResponse(data)


@app.get("/api/finance/refresh")
async def api_refresh():
    """Force refresh screener cache."""
    from pathlib import Path
    cache_file = Path("/tmp/market_pulse_cache/screener.json")
    if cache_file.exists():
        cache_file.unlink()
    data = get_stock_screener_data()
    return JSONResponse({"count": len(data), "status": "refreshed"})

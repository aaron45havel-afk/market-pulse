"""Market Pulse — Real Estate & Finance Dashboard."""
import os, logging, sqlite3
from contextlib import asynccontextmanager
from datetime import date
from pathlib import Path
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import JSONResponse, RedirectResponse, Response
from fastapi.middleware.gzip import GZipMiddleware
from dotenv import load_dotenv
from data_providers import STATES
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
        return date.fromisoformat(iso).strftime("%b %-d, %Y")
    except (ValueError, TypeError):
        return iso


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Market Pulse starting up...")
    init_db()
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
            "is_stub": bool(cfg.get("is_stub")),
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
    # Admin-only. Non-admins land back on the home/map view rather than
    # seeing a 401 — keeps the gating invisible to general visitors,
    # who don't see the nav link in the first place. Use /admin/login
    # to set the cookie before visiting.
    if not _check_admin_token(request):
        return RedirectResponse(url="/map", status_code=302)
    return templates.TemplateResponse("finance.html", {"request": request})


# ─── Stock lookup (public) ──────────────────────────────────────────
# Single-ticker search: Yahoo Finance for live quote + 1Y chart, SEC
# EDGAR for latest annual fundamentals. Public — no admin gate.

@app.get("/stocks")
async def stocks_page(request: Request):
    return templates.TemplateResponse("stocks.html", {"request": request})


@app.get("/api/stock/{ticker}/quote")
async def api_stock_quote(ticker: str):
    from stock_lookup import get_quote
    return JSONResponse(get_quote(ticker))


@app.get("/api/stock/{ticker}/fundamentals")
async def api_stock_fundamentals(ticker: str):
    from stock_lookup import get_fundamentals
    return JSONResponse(get_fundamentals(ticker))


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
    """Compares against the ADMIN_TOKEN env var. Accepts the token
    from any of three sources, in order of precedence:
      • ?token=<...>  query param (programmatic admin endpoints like
                                    /admin/signups?token=... CSV exports)
      • X-Admin-Token  request header (same use case via curl)
      • mp_admin       browser cookie set by /admin/login (UI gating —
                       hides /finance + /results from non-admins)

    Returns False when ADMIN_TOKEN env var is unset, so an accidental
    deploy without the secret refuses access rather than letting
    everyone in."""
    expected = os.environ.get("ADMIN_TOKEN", "").strip()
    if not expected:
        return False
    provided = (
        request.query_params.get("token", "") or
        request.headers.get("x-admin-token", "") or
        request.cookies.get(ADMIN_COOKIE, "")
    ).strip()
    return provided != "" and provided == expected


@app.get("/admin/login")
async def admin_login(request: Request, token: str = "", redirect: str = "/"):
    """Set the mp_admin cookie if ?token=<ADMIN_TOKEN> matches, then
    redirect (default: home). Visit /admin/login?token=<your-token>
    once and the admin nav links + pages unlock for 30 days.
    Bad/missing token → 401."""
    expected = os.environ.get("ADMIN_TOKEN", "").strip()
    if not expected or token != expected:
        return JSONResponse({"error": "Invalid token."}, status_code=401)
    resp = RedirectResponse(url=redirect, status_code=302)
    resp.set_cookie(
        key=ADMIN_COOKIE,
        value=token,
        max_age=60 * 60 * 24 * 30,   # 30 days
        httponly=True,
        # secure=True only over HTTPS so dev/test on http://localhost
        # still round-trips the cookie. Production on Railway is HTTPS
        # so this lights up automatically.
        secure=(request.url.scheme == "https"),
        samesite="lax",
    )
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
            (12, row["forecast_home_value_12mo"]),
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

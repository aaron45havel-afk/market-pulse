"""Market Pulse — Real Estate & Finance Dashboard."""
import asyncio, hmac, json, math, os, logging, sqlite3
from contextlib import asynccontextmanager
from datetime import date
from pathlib import Path
from fastapi import FastAPI, Query, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import JSONResponse, RedirectResponse, Response
from fastapi.middleware.gzip import GZipMiddleware
from dotenv import load_dotenv
from data_providers import STATES, MORTGAGE_30Y_RATE, qualifying_income
from sec_edgar import build_net_net_screener
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

# ── the multifamily ops platform ──
# ARCHITECTURE.md §3: "main.py gains one include_router call per portal
# and nothing else." One router covers all four, so this is that line.
# Wrapped because the ops half must never be why the analysis boards
# fail to start — the same fail-open reasoning as lib/ops/bootstrap.py,
# and it stops applying the moment these routes carry real traffic.
try:
    import routers.ops
    routers.ops.attach(app)
except Exception as _ops_err:      # pragma: no cover - defensive
    logger.error("ops routes not mounted: %s", _ops_err)


@app.get("/")
async def home():
    """Map-first landing — the national overview is the primary view; the
    other tools (state data, affordability, screener, results) are one
    click away in the sidebar. Permanent redirect so search engines and
    bookmarks settle on /map as the canonical home URL."""
    return RedirectResponse(url="/map", status_code=308)


@app.get("/map")
def zip_map(request: Request):
    """The national ZIP map: every Census ZCTA in data/zip_profile.db, one
    metric at a time — published figures, or the ZIP page's underwriting at
    the defaults the page states. No score. Points and values come from
    /api/map/base and /api/map/metric/{key}; each ZIP links to /zip/{zip}."""
    import map_data as MD
    import underwrite as U
    from data_providers import MORTGAGE_30Y_RATE, MORTGAGE_30Y_OBS_DATE
    meta = MD.load()[1]
    return templates.TemplateResponse("map.html", {
        "request": request, "registry": MD.registry(), "bases": MD.BASES,
        "default_metric": MD.DEFAULT_METRIC, "default_basis": MD.DEFAULT_BASIS,
        "defaults": U.DEFAULTS, "rate": MORTGAGE_30Y_RATE,
        "rate_date": _fmt_obs_date(MORTGAGE_30Y_OBS_DATE), "meta": meta,
    })


@app.get("/api/map/base")
def api_map_base():
    """Every ZIP's position, state and place name, in the order every
    /api/map/metric array follows."""
    import map_data as MD
    return JSONResponse(MD.base(), headers={"Cache-Control": "public, max-age=3600"})


@app.get("/api/map/metric/{key}")
def api_map_metric(key: str, basis: str = "zillow"):
    """One metric for every ZIP (see map_data.metric_values)."""
    import map_data as MD
    from data_providers import MORTGAGE_30Y_RATE, MORTGAGE_30Y_OBS_DATE
    out = MD.metric_values(key, basis, MORTGAGE_30Y_RATE, _fmt_obs_date(MORTGAGE_30Y_OBS_DATE))
    if out is None:
        return JSONResponse({"error": f"unknown metric {key!r}"}, status_code=404)
    return JSONResponse(out, headers={"Cache-Control": "public, max-age=3600"})


@app.get("/map/classic")
async def map_classic():
    """The previous map (metro pins, composite scores) was retired in the
    map rebuild's last phase; old links land on the ZIP map."""
    return RedirectResponse(url="/map", status_code=301)


@app.get("/real-estate")
async def real_estate():
    """Permanent redirect to /map. The standalone State Data dashboard
    was retired once the country → state → metro drill-down landed on
    /map (Phase 1, P96): state pills became the choropleth, the
    Goldilocks rankings became the State Info card's persona row, and
    the FRED-driven metric cards were already duplicated in the /map
    sidebar."""
    return RedirectResponse(url="/map", status_code=308)


@app.get("/real-estate/{slug}/map")
async def state_map(slug: str):
    """The 112 hand-curated metro maps (composite-scored) were retired with
    the old map. A metro slug ('TX', 'UT-STG') lands on the ZIP map
    filtered to its state."""
    st = slug.split("-")[0].upper()
    return RedirectResponse(url=f"/map?st={st}" if st in STATES else "/map", status_code=301)


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


@app.get("/fcf-quality")
async def fcf_quality_page(request: Request):
    """FCF quality — VFLO's two-stage funnel over the whole market.

    Reads from data/fcf_quality_snapshots/ (monthly, built by Action)."""
    return templates.TemplateResponse("fcf_quality.html", {"request": request})


@app.get("/lynch")
async def lynch(request: Request):
    """Peter Lynch GARP screener — large-cap value growth.
    Reads from data/lynch_snapshots/ (monthly cron-built)."""
    return templates.TemplateResponse("lynch.html", {"request": request})


@app.get("/hundred")
async def hundred(request: Request):
    """The 100-bagger checklist — a reading list, not a ranking.
    Reads from data/hundred_snapshots/ (monthly cron-built)."""
    return templates.TemplateResponse("hundred.html", {"request": request})


@app.get("/catalysts")
async def catalysts_page(request: Request, price: str = "", offer: str = "",
                         days: str = "", spread: str = "", shares: str = "",
                         oddlot: str = "0", account: str = "taxable",
                         completion: str = "", downside: str = "",
                         target: str = "15", mindollars: str = "2000",
                         rate: str = "37", ltcg: str = "20"):
    """Special-situation triage — which filing to read first.

    Deliberately NOT a buy signal. It ranks candidates by annualized
    return net of the round-trip spread and after tax at the holding
    period, shows the dollars actually at stake beside the percentage,
    and kills the ones that cannot pay for the evening they would cost.
    See catalysts.py for why there is no blended score.

    The queue comes from data/catalysts.json (CI-built from the EDGAR
    daily index). The calculator below it works with nothing loaded —
    deal terms come from reading the filing, which no feed can do."""
    import json as _json
    import catalysts as CT

    path = Path(__file__).resolve().parent / "data" / "catalysts.json"
    queue, meta, insiders, routine = [], None, [], []
    if path.exists():
        blob = _json.loads(path.read_text())
        queue, meta = blob.get("rows", []), blob.get("_meta")
        # Absent in payloads written before Form 4s were resolved per
        # filing. An older file degrades to the deal queue alone rather
        # than erroring, so a stale deploy still renders.
        insiders, routine = blob.get("insiders", []), blob.get("routine", [])

    target_n = max(0.0, min(500.0, _qnum(target, 15.0)))
    mind_n = max(0.0, min(10_000_000.0, _qnum(mindollars, 2000.0)))
    ord_rate = max(0.0, min(60.0, _qnum(rate, 37.0))) / 100.0
    ltcg_rate = max(0.0, min(60.0, _qnum(ltcg, 20.0))) / 100.0
    account = account if account in ("taxable", "ira", "roth") else "taxable"

    price_n, offer_n = _qnum(price), _qnum(offer)
    days_n = _qnum(days)
    result = verdict = compare = None
    if price_n > 0 and offer_n > 0 and days_n > 0:
        comp = _qnum(completion)
        down = _qnum(downside)
        kw = dict(price=price_n, consideration=offer_n, days=days_n,
                  roundtrip_spread_pct=max(0.0, _qnum(spread)),
                  shares=(int(_qnum(shares)) or None),
                  odd_lot=_qnum(oddlot) > 0, account=account,
                  completion=(comp if 0 < comp <= 1 else None),
                  downside=(down if down > 0 else None),
                  ordinary_rate=ord_rate, ltcg_rate=ltcg_rate)
        result = CT.evaluate(**kw)
        verdict = CT.triage(result, target_after_tax_pct=target_n, min_dollars=mind_n)
        compare = CT.account_comparison(**kw)

    return templates.TemplateResponse("catalysts.html", {
        "request": request, "queue": queue, "meta": meta,
        "insiders": insiders, "routine": routine,
        "catalog": CT.CATALYSTS, "result": result, "verdict": verdict,
        "compare": compare, "price": price, "offer": offer, "days": days,
        "spread": spread, "shares": shares, "oddlot": _qnum(oddlot) > 0,
        "account": account, "completion": completion, "downside": downside,
        "target": target_n, "mindollars": mind_n,
        "rate": _qnum(rate, 37.0), "ltcg": _qnum(ltcg, 20.0),
    })


def quiet_value_board(rows: list[dict], liq: str = "low", size: str = "", minpass_n: int = 5,
                      want_tradeable: bool = True, excl_high: bool = True,
                      require_t: tuple = ()) -> tuple[list[dict], dict]:
    """The /quiet-value funnel: the rows that clear every filter, best first,
    and how many survived each step — counted before filtering so a short
    board reads as a strict screen rather than an empty market."""
    import liquidity as LQ
    import quality_value as QV

    counts = {"all": len(rows)}
    out = [r for r in rows if LQ.passes(r, max_bucket=liq, exclude_high=excl_high)]
    counts["after_liquidity"] = len(out)
    if size:
        out = [r for r in out if r.get("size_bucket") == size]
    counts["after_size"] = len(out)
    if want_tradeable:
        out = [r for r in out if not r.get("impractical")]
    counts["after_tradeable"] = len(out)
    # Banks, insurers and funds stop here: the tests that do not apply to
    # them (quality_value.NOT_APPLICABLE) leave fewer than five to measure.
    # Counted so the funnel can say so.
    counts["financials"] = sum(1 for r in out if r.get("industry") in QV.CANNOT_QUALIFY)
    out = [r for r in out
           if r.get("known", 0) >= 5
           and r.get("passed", 0) >= minpass_n
           and all(r.get("verdicts", {}).get(t) for t in require_t)]
    counts["after_quality"] = len(out)
    out.sort(key=lambda r: (-(r.get("passed") or 0),
                            r.get("turnover") if r.get("turnover") is not None else 9e9))
    return out, counts


@app.get("/quiet-value")
async def quiet_value_page(request: Request, liq: str = "low", size: str = "",
                           minpass: str = "5", require: str = "",
                           tradeable: str = "1", exclude_high: str = "1"):
    """Quiet-value screen: the low-turnover end of the small-cap market,
    paired with a balance-sheet and valuation test.

    The liquidity axis is turnover (12m volume / shares outstanding),
    cut into quartiles WITHIN each size band — see liquidity.py for why
    cutting globally would collapse it into a second size axis. The
    high-turnover quartile is excluded by default rather than merely
    ranked last: in the research it is the cell that earned ~0.1%/yr.

    Everything here is precomputed by scripts/refresh_quiet_value.py in
    CI, because it needs a year of daily volume per ticker."""
    import json as _json
    import liquidity as LQ
    import quality_value as QV

    path = Path(__file__).resolve().parent / "data" / "quiet_value.json"
    if not path.exists():
        return templates.TemplateResponse("quiet_value.html", {
            "request": request, "rows": [], "meta": None, "pending": True,
            "liq": liq, "size": size, "minpass": 5, "require": require,
            "tradeable": True, "exclude_high": True, "tests": QV.TESTS,
            "labels": QV.LABELS, "na_reason": QV.NA_REASON, "counts": {}, "shown": 0,
        })
    blob = _json.loads(path.read_text())
    rows = blob.get("rows", [])

    liq = liq if liq in LQ.QUARTILE_NAMES else "low"
    size = size if size in {b[0] for b in LQ.SIZE_BANDS} else ""
    minpass_n = max(0, min(7, int(_qnum(minpass, 5))))
    want_tradeable = _qnum(tradeable, 1) > 0
    excl_high = _qnum(exclude_high, 1) > 0
    require_t = tuple(t for t in require.split(",") if t in QV.TESTS)

    out, counts = quiet_value_board(rows, liq, size, minpass_n, want_tradeable, excl_high, require_t)
    return templates.TemplateResponse("quiet_value.html", {
        "request": request, "rows": out[:200], "meta": blob.get("_meta"),
        "pending": False, "liq": liq, "size": size, "minpass": minpass_n,
        "require": require, "tradeable": want_tradeable, "exclude_high": excl_high,
        "tests": QV.TESTS, "labels": QV.LABELS, "na_reason": QV.NA_REASON,
        "counts": counts, "shown": len(out),
    })


@app.get("/norcal")
async def norcal_page(request: Request, assets: str = "200000",
                      reserves: str = "40000", down_pct: str = "20",
                      region: str = "", market: str = "CA",
                      zip: str = "", price: str = "", sqft: str = "",
                      income: str = "", safetier: str = "safe",
                      unknown: str = "0", surge: str = "0", winter: str = "moderate",
                      foodpct: str = "75", access: str = "30"):
    """Home-buying strict screen across markets (California / New England).

    The safety gate runs on REAL FBI city crime rates (safety.py), never
    the income-derived crime_index — see norcal.screen for why that
    matters. Dining is percentile-calibrated within the market, climate
    and coastal hazards come from the market's own researched layer."""
    from norcal import screen, deal_check
    import regions as RG
    market = market if market in RG.MARKETS else "CA"
    mkt = RG.MARKETS[market]
    assets_n = max(0.0, min(50_000_000, _qnum(assets, 200_000)))
    reserves_n = max(0.0, min(assets_n, _qnum(reserves, 40_000)))
    down_n = max(3.0, min(100.0, _qnum(down_pct, 20)))
    if region not in mkt["regions"]:
        region = mkt["default_region"]
    res = await asyncio.to_thread(
        screen, assets_n, reserves_n, down_n, 33.0, 45.0,
        max(5.0, min(120.0, _qnum(access, 30))), region, market,
        safetier, _qnum(unknown) > 0, _qnum(surge) > 0,
        max(0.0, min(99.0, _qnum(foodpct, 75))),
        winter if winter in RG.CLIMATE_TIERS else "moderate")
    import screen_history as SH
    changes = await asyncio.to_thread(SH.diff, market)
    freshness = SH.layer_freshness()
    check = None
    price_n = _qnum(price)
    if zip and price_n > 0:
        check = await asyncio.to_thread(
            deal_check, zip.strip(), price_n, _qnum(sqft) or None,
            _qnum(income) or None, assets_n, reserves_n, down_n)
    return templates.TemplateResponse("norcal.html", {
        "request": request, "res": res, "power": res["power"], "check": check,
        "regions": mkt["regions"], "market": market, "markets": RG.MARKETS,
        "safetier": safetier, "allow_unknown": _qnum(unknown) > 0,
        "allow_surge": _qnum(surge) > 0, "foodpct": _qnum(foodpct, 75),
        "changes": changes, "freshness": freshness,
        "stale_layers": [f for f in freshness if f["stale"]],
        "winter": winter if winter in RG.CLIMATE_TIERS else "moderate",
        "access_max": _qnum(access, 30),
    })


def _qnum(v: str | None, default: float = 0.0) -> float:
    """Tolerant query-param number: '' / junk → default (an empty optional
    <input type=number> submits as an empty string, which must never 422).
    Non-finite values ('inf', '1e309', 'nan') also fall back — float()
    accepts them but int()/round() downstream raise OverflowError."""
    try:
        s = (v or "").strip()
        f = float(s) if s else default
        return f if math.isfinite(f) else default
    except ValueError:
        return default


async def _build_remodel_budget(bsqft: str, bbeds: str, bbaths: str, byear: str,
                                bscope: str, blevel: str, bstate: str, bzip: str,
                                bconv: str, bmasonry: str, bfound: str, bwin: str):
    """Shared by /value-add and the contractor-plan PDF so both render the
    exact same budget from the same query params. Returns (budget, bmarket,
    loc) — all None when no sqft was given."""
    from value_add import remodel_budget, locate_market
    bsqft_n, bbeds_n, bbaths_n = _qnum(bsqft), int(_qnum(bbeds, 3)), _qnum(bbaths, 1)
    byear_n, bwin_n = int(_qnum(byear)), int(_qnum(bwin))
    if bsqft_n <= 0:
        return None, None, None
    loc = None
    if bzip.strip():
        loc = await asyncio.to_thread(locate_market, bzip)
    bmarket = loc["market"] if loc else None
    auto = bstate.strip().upper() in ("", "AUTO")
    effective_state = (loc["code"] if (auto and loc) else ("CA-BAY" if auto else bstate))
    budget = await asyncio.to_thread(
        remodel_budget, bsqft_n, bbeds_n, bbaths_n, (byear_n or None), bscope, blevel,
        state=effective_state, conversion=bool(_qnum(bconv)), masonry=bool(_qnum(bmasonry)),
        foundation_replace=bool(_qnum(bfound)), windows=(bwin_n or None))
    return budget, bmarket, loc


@app.get("/value-add")
async def value_add_page(request: Request, region: str = "All CA",
                         zip: str = "", price: str = "", units: str = "2",
                         rehab: str = "", rent: str = "", income: str = "",
                         bsqft: str = "", bbeds: str = "3", bbaths: str = "1",
                         byear: str = "", bscope: str = "gut", blevel: str = "mid",
                         bstate: str = "AUTO", bprice: str = "", bzip: str = "", barv: str = "",
                         bgreen: str = "20", bred: str = "8",
                         bconv: str = "0", bmasonry: str = "0", bfound: str = "0", bwin: str = ""):
    """CA needs-work multifamily finder: hunting-ground ZIP ranking +
    203(k)-first rehab underwriting + a metro-aware SFR full-remodel budget
    builder with a buy / no-buy verdict. Paste the property address and the
    budgeter resolves the closest metro's costs itself. All numeric query
    params are parsed tolerantly — empty strings fall back to defaults
    instead of failing validation. See value_add.py."""
    from value_add import (hunting_grounds, rehab_check, flip_verdict,
                           REMODEL_SCOPES, REMODEL_LEVELS, STATE_NAMES)
    from norcal import REGIONS
    if region not in REGIONS:
        region = "All CA"
    price_n, rehab_n = _qnum(price), _qnum(rehab)
    rent_n, income_n = _qnum(rent), _qnum(income)
    bprice_n, barv_n = _qnum(bprice), _qnum(barv)
    bgreen_n, bred_n = _qnum(bgreen, 20), _qnum(bred, 8)
    res = await asyncio.to_thread(hunting_grounds, region)
    check = None
    if zip and price_n > 0:
        check = await asyncio.to_thread(
            rehab_check, zip.strip(), price_n, int(_qnum(units, 2)), max(0.0, rehab_n),
            rent_n or None, income_n or None)
    verdict = None
    budget, bmarket, loc = await _build_remodel_budget(
        bsqft, bbeds, bbaths, byear, bscope, blevel, bstate, bzip,
        bconv, bmasonry, bfound, bwin)
    if budget is not None and bprice_n > 0:
        arv = barv_n if barv_n > 0 else (bmarket or {}).get("median_home_value")
        verdict = flip_verdict(bprice_n, budget["total"], arv, bgreen_n, bred_n)
    return templates.TemplateResponse("value_add.html", {
        "request": request, "res": res, "check": check, "budget": budget,
        "verdict": verdict, "bmarket": bmarket, "loc": loc,
        "bstate_raw": bstate,
        "bprice": bprice_n, "bzip": bzip, "barv": barv_n, "bgreen": bgreen_n, "bred": bred_n,
        "regions": REGIONS, "rehab": rehab_n,
        "scopes": REMODEL_SCOPES, "levels": REMODEL_LEVELS, "state_names": STATE_NAMES,
    })


@app.get("/value-add/contractor-plan.pdf")
async def contractor_plan_pdf(bsqft: str = "", bbeds: str = "3", bbaths: str = "1",
                              byear: str = "", bscope: str = "gut", blevel: str = "mid",
                              bstate: str = "AUTO", bzip: str = "",
                              bconv: str = "0", bmasonry: str = "0", bfound: str = "0",
                              bwin: str = "", bdollars: str = "1"):
    """Downloadable contractor scope-of-work: the current budget's line
    items grouped into construction phases in build order (see
    remodel_plan.py). Same query params as the budgeter so the page link
    just carries the form state over. bdollars=0 strips the allowance
    figures for clean competitive bids. Deal economics (asking price, ARV,
    margins) are never included — this document goes to the contractor."""
    budget, _bmarket, loc = await _build_remodel_budget(
        bsqft, bbeds, bbaths, byear, bscope, blevel, bstate, bzip,
        bconv, bmasonry, bfound, bwin)
    if budget is None:
        return RedirectResponse("/value-add#budgeter", status_code=302)
    from remodel_plan import plan_pdf
    pdf = await asyncio.to_thread(plan_pdf, budget, address=bzip.strip(),
                                  dollars=_qnum(bdollars, 1) > 0)
    fn = f"contractor-plan-{loc['zip'] if loc else budget['state'].lower()}.pdf"
    return Response(content=pdf, media_type="application/pdf",
                    headers={"Content-Disposition": f'attachment; filename="{fn}"'})


@app.get("/headroom")
async def headroom_page(request: Request, mode: str = "brrrr", scope: str = "moderate",
                        level: str = "low", target: str = "14", rate: str = "",
                        sqft: str = "1500", appr: str = "0", metro: str = "",
                        universe: str = "all", hstate: str = "", units: str = "4",
                        maxprice: str = "300000", safetier: str = "safe",
                        unknown: str = "0"):
    """Headroom — the remodel deal engine. For every market, the most you
    can pay for a fixer and still clear the after-tax return target and the
    $25k floor (BRRRR or flip), as a share of its median home, and which
    limit sets it; or, by ZIP, an owner-occupant house-hack's max offer.
    See headroom.py. All numeric params parsed tolerantly."""
    import headroom as HR
    import screen_history as SH
    from data_providers import MORTGAGE_30Y_RATE
    mode = mode if mode in ("brrrr", "flip", "hh") else "brrrr"
    # The research tables this page reads, dated, stale ones flagged.
    freshness = [f for f in SH.layer_freshness()
                 if f["layer"] in HR.LAYERS + (("crime",) if mode == "hh" else ())]
    scope = scope if scope in ("cosmetic", "moderate", "gut") else "moderate"
    level = level if level in ("low", "mid", "high") else "low"
    target_n = max(1.0, min(60.0, _qnum(target, 14)))
    rate_n = _qnum(rate) or MORTGAGE_30Y_RATE or 6.55
    sqft_n = max(600.0, min(6000.0, _qnum(sqft, 1500)))
    # Exit appreciation, %/yr: the user's, 0 unless set. A trailing price
    # trend is shown as history, never used as the forecast.
    appr_n = max(-5.0, min(5.0, _qnum(appr, 0)))
    units_n = max(2, min(4, int(_qnum(units, 4))))
    maxprice_n = max(50_000.0, min(2_000_000.0, _qnum(maxprice, 300_000)))
    hstate = hstate.strip().upper()[:2]
    common = {"request": request, "mode": mode, "scope": scope, "level": level,
              "target": target_n, "rate": rate_n, "sqft": sqft_n, "appr": appr_n,
              "universe": universe, "calib": HR.calibration(scope),
              "fin": HR.financing_terms(rate_n), "profit_floor": HR.PROFIT_FLOOR,
              "hold_years": HR.HOLD_YEARS, "freshness": freshness,
              "vacancy": HR.VACANCY, "maint": HR.MAINTENANCE_PCT}
    if mode == "hh":
        # Owner-occupant house-hack: ZIP-level, no DSCR (FHA self-sufficiency
        # is the funding gate), solved for the max offer per ZIP, gated on
        # REAL city-level crime data (safety.py) — unknown never passes as
        # safe unless the user explicitly opts in.
        import safety as SF
        safetier = SF.valid_tier(safetier)
        allow_unknown = _qnum(unknown) > 0
        hh_board = await asyncio.to_thread(
            HR.zip_board_hh, state=(hstate or None), units=units_n, scope=scope,
            level=level, rate_pct=rate_n, max_price=maxprice_n,
            max_tier=safetier, allow_unknown=allow_unknown)
        return templates.TemplateResponse("headroom.html", {
            **common, "board": [], "hh_board": hh_board, "summary": None,
            "metro": "", "drill": None, "metro_name": "",
            "hstate": hstate, "units": units_n, "maxprice": maxprice_n,
            "safetier": safetier, "allow_unknown": allow_unknown,
            "crime_cov": SF.coverage(), "us_violent": SF.US_VIOLENT,
            "unit_factor": HR.HH.UNIT_PRICE_FACTOR.get(units_n),
        })
    board = await asyncio.to_thread(
        HR.build_board, mode=mode, scope=scope, level=level, target=target_n,
        rate_pct=rate_n, sqft=sqft_n, appreciation=appr_n / 100.0,
        metros_only=(universe == "metros"))
    drill = None
    metro = metro.strip().upper()
    if metro:
        drill = await asyncio.to_thread(
            HR.zip_drilldown, metro, mode=mode, scope=scope, level=level,
            target=target_n, rate_pct=rate_n, sqft=sqft_n, appreciation=appr_n / 100.0)
    return templates.TemplateResponse("headroom.html", {
        **common, "board": board, "summary": HR.board_summary(board),
        "metro": metro, "drill": drill,
        "metro_name": next((r["name"] for r in board if r["code"] == metro), metro),
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
            # `fresh` is False when this statement's balance is OLDER than one we
            # already hold — in that case don't let its balance clobber the
            # settings mirror the decision engine reads (the account layer
            # already refuses the stale write).
            fresh = True
            if bal is not None:
                # date the balance by the statement's latest activity, so an
                # older statement can't clobber a newer cash-on-hand figure
                bal_date = max((t["date"] for t in txns if t.get("date")), default=None)
                fresh = household_set_account_balance(code, aid, bal, bal_date)
            if acct["kind"] == "heloc":
                s = acct["summary"]
                if fresh and s.get("balance") is not None:
                    settings_update["heloc_balance"] = s["balance"]
                if s.get("apr") is not None:
                    settings_update["heloc_apr"] = s["apr"]
                if s.get("min_payment") is not None:
                    settings_update["heloc_payment"] = s["min_payment"]
                if s.get("credit_limit") is not None:
                    settings_update["heloc_limit"] = s["credit_limit"]
            elif acct["kind"] == "credit_card":
                s = acct["summary"]
                if fresh and s.get("balance") is not None:
                    settings_update["card_balance"] = s["balance"]
                if s.get("apr") is not None:
                    settings_update["card_apr"] = s["apr"]
                if s.get("min_payment") is not None:
                    settings_update["card_payment"] = s["min_payment"]
            elif acct["kind"] == "loan":
                # Used Auto (car loan) → auto_*, Personal Line → loc_*, so the
                # decision + opportunity-cost engines pick them up with no
                # manual entry. NOTE: one slot per class, so a household with two
                # cars (or two lines) would collapse onto one key — fine for the
                # single-loan case this serves; multiple same-class loans would
                # need a list here and in the engines.
                s = acct["summary"]
                nl = acct["name"].lower()
                prefix = "auto" if "auto" in nl else ("loc" if "line" in nl else None)
                if prefix:
                    if fresh and s.get("balance") is not None:
                        settings_update[f"{prefix}_balance"] = s["balance"]
                    if s.get("apr") is not None:
                        settings_update[f"{prefix}_apr"] = s["apr"]
                    # Recurring monthly payment: the stated minimum (revolving
                    # line) or the MEDIAN of the period's loan payments (an
                    # installment loan lists no minimum). Median, not max, so a
                    # one-off extra principal payment doesn't inflate the figure
                    # the payoff projection relies on.
                    pmts = sorted(abs(t["amount"]) for t in txns
                                  if t.get("bucket") == "Loan Payment")
                    pay = s.get("min_payment")
                    if pay is None and pmts:
                        pay = pmts[len(pmts) // 2]
                    if pay is not None:
                        settings_update[f"{prefix}_payment"] = pay
                    if prefix == "loc" and s.get("credit_limit") is not None:
                        settings_update["loc_limit"] = s["credit_limit"]
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


@app.get("/api/household/books/{code}/bills")
async def api_hh_bills(code: str):
    """Her committed monthly nut — recurring fixed + debt bills grouped by
    payee, with a per-category rollup and the total."""
    import household
    from database import household_all_txns
    bad = await _require_hh_book(code)
    if bad:
        return bad

    def _do():
        rows = household_all_txns(code)
        for r in rows:
            r["cls"] = household.bucket_class(r["bucket"])
        return household.fixed_bills(rows)
    return JSONResponse(await asyncio.to_thread(_do))


@app.get("/api/household/books/{code}/coverage")
async def api_hh_coverage(code: str):
    """Which months are imported per account — and which are missing — so a
    skipped statement (a forgotten HELOC month) is easy to spot."""
    import household
    from database import household_all_txns, household_list_accounts
    bad = await _require_hh_book(code)
    if bad:
        return bad

    def _do():
        rows = household_all_txns(code)
        accounts = household_list_accounts(code)
        return {"accounts": household.statement_coverage(rows, accounts)}
    return JSONResponse(await asyncio.to_thread(_do))


@app.get("/api/household/books/{code}/merchants")
async def api_hh_merchants(code: str, q: str = ""):
    """Where the money goes by store — top merchants, plus a total for a
    search term (a store like 'costco' or a category like 'gas')."""
    import household
    from database import household_all_txns
    bad = await _require_hh_book(code)
    if bad:
        return bad

    def _do():
        rows = household_all_txns(code)
        for r in rows:
            r["cls"] = household.bucket_class(r["bucket"])
        return {"top": household.top_merchants(rows),
                "lookup": household.spend_lookup(rows, q) if q.strip() else None}
    return JSONResponse(await asyncio.to_thread(_do))


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
               "auto_balance", "auto_apr", "auto_payment",
               "loc_balance", "loc_apr", "loc_payment", "loc_limit",
               "invest_return", "debt_free_target_year", "home_value"}
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


def _current_month():
    import datetime
    return datetime.date.today().strftime("%Y-%m")


def _assemble_roadmap(code):
    """Shared roadmap assembly — vitals + reno summary + retirement summary
    into money_roadmap, plus the raw pieces the monthly checklist reuses."""
    import household
    from database import (household_all_txns, household_get_settings,
                          household_list_projects, household_budget_items,
                          household_list_accounts)
    rows = household_all_txns(code)
    for r in rows:
        r["cls"] = household.bucket_class(r["bucket"])
    settings = household_get_settings(code)
    accounts = household_list_accounts(code)
    vitals = household.vital_signs(rows, settings, accounts)

    reno = {"active": False, "budget_total": 0.0, "can_fund": 0.0}
    projects = household_list_projects(code)
    for p in projects:
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

    plan = household.retirement_plan(settings)
    retire = {"configured": False}
    if plan.get("configured") and plan.get("chosen"):
        ch = plan["chosen"]
        retire = {"configured": True, "year": ch["year"],
                  "covered": ch["covered"], "surplus": ch["surplus_with_ss"]}

    roadmap = household.money_roadmap(vitals, reno=reno, retire=retire)
    return {"roadmap": roadmap, "vitals": vitals, "reno": reno,
            "rows": rows, "settings": settings, "projects": projects}


@app.get("/api/household/books/{code}/roadmap")
async def api_hh_roadmap(code: str):
    """The framework: one ordered path over cash flow, debts, the kitchen
    goal and retirement — with the single next move she should focus on."""
    bad = await _require_hh_book(code)
    if bad:
        return bad
    ctx = await asyncio.to_thread(_assemble_roadmap, code)
    return JSONResponse(ctx["roadmap"])


@app.get("/api/household/books/{code}/checklist")
async def api_hh_checklist(code: str):
    """This month's concrete, checkable routine, derived from her state."""
    import household
    bad = await _require_hh_book(code)
    if bad:
        return bad

    def _do():
        ctx = _assemble_roadmap(code)
        rows, settings, projects = ctx["rows"], ctx["settings"], ctx["projects"]
        vitals, roadmap = ctx["vitals"], ctx["roadmap"]
        uncategorized = sum(1 for r in rows if r.get("bucket") == "Uncategorized")
        payees = settings.get("reno_payees") or []
        labor_pending = sum(len(household.labor_candidates(rows, p.get("start"), p.get("end"), payees))
                            for p in projects)
        current = next((s for s in roadmap["steps"] if s.get("status") == "now"), None)
        items = household.monthly_checklist({
            "current_step": current, "surplus": vitals.get("avg_net"),
            "uncategorized": uncategorized, "reno_active": ctx["reno"]["active"],
            "labor_pending": labor_pending,
            "card_balance": vitals.get("card_balance"),
            "heloc_balance": vitals.get("heloc_balance"),
        })
        month = _current_month()
        done = set((settings.get("checklist") or {}).get(month, []))
        for it in items:
            it["done"] = it["key"] in done
        return {"month": month, "items": items,
                "done_count": sum(1 for it in items if it["done"]), "total": len(items)}

    return JSONResponse(await asyncio.to_thread(_do))


@app.post("/api/household/books/{code}/checklist")
async def api_hh_checklist_toggle(code: str, request: Request):
    """Tick (or untick) a checklist item for the current month."""
    from database import household_get_settings, household_set_settings
    bad = await _require_hh_book(code)
    if bad:
        return bad
    body = await request.json()
    key = str(body.get("key") or "").strip()
    if not key:
        return JSONResponse({"error": "key required"}, status_code=400)
    done = bool(body.get("done"))

    def _do():
        settings = household_get_settings(code)
        chk = settings.get("checklist") or {}
        month = _current_month()
        cur = set(chk.get(month, []))
        cur.add(key) if done else cur.discard(key)
        chk[month] = sorted(cur)
        settings["checklist"] = chk
        household_set_settings(code, settings)
        return {"month": month, "done": sorted(cur)}

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
    num_fields = {"birth_year", "birth_month", "retire_expenses", "mortgage_balance",
                  "mortgage_payment", "mortgage_rate", "retire_year", "ss_claim_age",
                  "cola_rate", "cost_inflation"}
    clean = {}
    for k, v in incoming.items():
        if k in num_fields:
            fv = _coerce_float(v)
            if fv is not None:
                # ss_claim_age is fractional (67.25 = 67 yrs 3 mo); the rest of
                # the whole-number fields stay ints.
                clean[k] = int(fv) if k in ("birth_year", "birth_month", "retire_year") else fv
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
        for k, v in clean.items():
            # merge schedule dicts so a partial edit doesn't wipe other years
            if k in ("pension_by_year", "ss_by_age") and isinstance(ret.get(k), dict):
                merged = dict(ret[k]); merged.update(v); ret[k] = merged
            else:
                ret[k] = v
        cur["retirement"] = ret
        household_set_settings(code, cur)
        return household.retirement_plan(cur)
    return JSONResponse(await asyncio.to_thread(_do))


@app.get("/api/household/books/{code}/rental")
async def api_hh_rental(code: str):
    """'What if she rented the house?' cash-flow model."""
    import household
    from database import household_get_settings
    bad = await _require_hh_book(code)
    if bad:
        return bad
    settings = await asyncio.to_thread(household_get_settings, code)
    return JSONResponse(household.rental_scenario(settings))


@app.post("/api/household/books/{code}/rental")
async def api_hh_rental_save(code: str, request: Request):
    """Save rental assumptions (rent, management/vacancy/maintenance %)."""
    import household
    from database import household_get_settings, household_set_settings
    bad = await _require_hh_book(code)
    if bad:
        return bad
    body = await request.json()
    incoming = body if isinstance(body, dict) else {}
    allowed = {"rental_rent", "rental_mgmt_pct", "rental_vacancy_pct", "rental_maint_pct"}
    clean = {}
    for k in allowed:
        if k in incoming:
            fv = _coerce_float(incoming[k])
            if fv is not None:
                clean[k] = fv

    def _do():
        cur = household_get_settings(code)
        cur.update(clean)
        household_set_settings(code, cur)
        return household.rental_scenario(cur)
    return JSONResponse(await asyncio.to_thread(_do))


@app.post("/api/household/books/{code}/income-label")
async def api_hh_income_label(code: str, request: Request):
    """Give a cryptic income deposit a friendly name (e.g. rename
    'PENSION BENEFITS PENBEJUL26' to 'Nursing pension')."""
    from database import household_get_settings, household_set_settings
    bad = await _require_hh_book(code)
    if bad:
        return bad
    body = await request.json()
    key = str(body.get("key") or "").strip()
    name = str(body.get("name") or "").strip()
    if not key:
        return JSONResponse({"error": "key required"}, status_code=400)

    def _do():
        settings = household_get_settings(code)
        labels = settings.get("income_labels") or {}
        if name:
            labels[key] = name[:60]
        else:
            labels.pop(key, None)
        settings["income_labels"] = labels
        household_set_settings(code, settings)
        return {"income_labels": labels}
    return JSONResponse(await asyncio.to_thread(_do))


@app.post("/api/household/books/{code}/income-cola")
async def api_hh_income_cola(code: str, request: Request):
    """Set (or clear) the annual cost-of-living-adjustment percent on an
    income stream, so a paycheck can be broken into base pay + this year's
    COLA raise (e.g. her CalHR state raise). Body: {key, pct}."""
    from database import household_get_settings, household_set_settings
    bad = await _require_hh_book(code)
    if bad:
        return bad
    body = await request.json()
    key = str(body.get("key") or "").strip()
    if not key:
        return JSONResponse({"error": "key required"}, status_code=400)
    pct = _coerce_float(body.get("pct"))

    def _do():
        settings = household_get_settings(code)
        cola = settings.get("income_cola") or {}
        if pct and pct > 0:
            cola[key] = round(pct, 3)
        else:
            cola.pop(key, None)
        settings["income_cola"] = cola
        household_set_settings(code, settings)
        return {"income_cola": cola}
    return JSONResponse(await asyncio.to_thread(_do))


@app.post("/api/household/books/{code}/debt-boosts")
async def api_hh_debt_boosts(code: str, request: Request):
    """Set the scheduled step-ups in her debt-paydown capacity — e.g. a 401k
    loan finishing that frees $732/mo from Nov. Body: {boosts: [{month, amount,
    label}]}. Replaces the whole list (send [] to clear)."""
    from database import household_get_settings, household_set_settings
    bad = await _require_hh_book(code)
    if bad:
        return bad
    body = await request.json()
    incoming = body.get("boosts") if isinstance(body.get("boosts"), list) else []
    clean = []
    for b in incoming[:12]:
        if not isinstance(b, dict):
            continue
        month = str(b.get("month") or "").strip()
        amt = _coerce_float(b.get("amount"))
        if not re.match(r"^\d{4}-\d{2}$", month) or amt is None or amt <= 0:
            continue
        clean.append({"month": month, "amount": round(amt, 2),
                      "label": str(b.get("label") or "")[:60]})

    def _do():
        settings = household_get_settings(code)
        settings["debt_boosts"] = clean
        household_set_settings(code, settings)
        return {"debt_boosts": clean}
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
    boosts = _resolve_debt_boosts(settings.get("debt_boosts"))
    target_months = _debt_target_months(settings.get("debt_free_target_year"))
    return JSONResponse(household.this_month(rows, settings, mode, accounts,
                                             boosts=boosts, target_months=target_months))


def _month_index(year, month):
    return int(year) * 12 + int(month)


def _resolve_debt_boosts(raw):
    """Turn stored debt boosts ({month:'YYYY-MM', amount, label}) into the
    engine's {from_month, amount, label} — from_month = months from now until
    the boost is active (1-based, clamped so a past boost applies immediately)."""
    import datetime
    if not isinstance(raw, list):
        return []
    today = datetime.date.today()
    now = _month_index(today.year, today.month)
    out = []
    for b in raw:
        try:
            y, m = str(b.get("month")).split("-")[:2]
            amt = float(b.get("amount") or 0)
            fm = max(1, _month_index(int(y), int(m)) - now)   # keep int() inside the guard
        except (TypeError, ValueError, AttributeError):
            continue
        if amt <= 0:
            continue
        out.append({"from_month": fm, "amount": amt,
                    "label": str(b.get("label") or "")[:60], "month": b.get("month")})
    return out


def _debt_target_months(year):
    """Months from now until the END of the target year (the debt-free
    deadline). None when unset or already past."""
    import datetime
    if not year:
        return None
    try:
        year = int(year)
    except (TypeError, ValueError):
        return None
    today = datetime.date.today()
    months = _month_index(year, 12) - _month_index(today.year, today.month)
    return months if months >= 1 else None


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


@app.post("/api/household/books/{code}/projects/{pid}/budget/fit")
async def api_hh_budget_fit(code: str, pid: int):
    """Auto-pick the option combination that spends the most of her budget
    target without going over, and apply it."""
    import household
    from database import (household_budget_items, household_budget_choose)
    bad = await _require_hh_book(code)
    if bad:
        return bad

    def _do():
        items = household_budget_items(code, pid)
        meta = _budget_meta_with_financing(code, pid)
        opt = household.optimize_budget(items, meta)
        for iid in opt.get("picks", {}).values():
            household_budget_choose(code, iid)
        return opt
    return JSONResponse(await asyncio.to_thread(_do))


@app.post("/api/household/books/{code}/projects/{pid}/budget/lock-plan")
async def api_hh_budget_lock_plan(code: str, pid: int):
    """Snapshot the current estimates as the budgeted plan — the baseline
    reallocation (freed / over) is measured against."""
    from database import household_budget_lock_plan
    bad = await _require_hh_book(code)
    if bad:
        return bad
    n = await asyncio.to_thread(household_budget_lock_plan, code, pid)
    return JSONResponse({"locked": n})


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


@app.get("/capital")
async def capital_page(request: Request):
    """The highest and best use of the next dollar, as a dashboard: the money
    picture, a ranked Do-next list (each step can be marked done, which edits
    the profile), independence, debts, passive income, property, taxes, the
    board and the profile in five steps. Private: it reads the owner's pay,
    taxes, accounts and debts."""
    if not _check_admin_token(request):
        return RedirectResponse("/sign-in?redirect=/capital", status_code=303)
    import capital as K
    import capital_view as V
    from database import get_capital_profile
    saved = get_capital_profile() or {}
    fresh = request.query_params.get("fresh") == "1"         # "Refresh prices": past the 15-minute cache
    board = await asyncio.to_thread(lambda: K.build(saved, fresh_prices=fresh))
    view = await asyncio.to_thread(V.build_view, board)
    p = board["profile"]
    return templates.TemplateResponse("capital.html", {
        "request": request, "board": board, "p": p, "view": view, "saved": bool(saved),
        "updated_at": saved.get("_updated_at"), "last_step": saved.get("last_step"),
        "debts_text": K.debts_text(p.get("debts") or []), "limits": K.LIMITS_2026,
        "hours_default": K.HOURS_DEFAULT, "accounts": V.EDIT_ACCOUNT_LABELS,
    })


@app.post("/api/capital/profile")
async def capital_profile_save(request: Request):
    """Save the owner's profile (JSON body; any subset of fields). Admin only."""
    gate = _admin_gate(request)
    if gate:
        return gate
    import capital as K
    from database import get_capital_profile, save_capital_profile
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Expected a JSON body."}, status_code=400)
    merged = {**{k: v for k, v in (get_capital_profile() or {}).items() if not k.startswith("_")},
              **K.parse_profile(body if isinstance(body, dict) else {})}
    merged.pop("last_step", None)          # an edit by hand closes the undo banner
    if not save_capital_profile(merged):
        return JSONResponse({"error": "Could not save — the database is unavailable."}, status_code=503)
    return JSONResponse({"ok": True})


@app.post("/api/capital/step")
async def capital_step_done(request: Request):
    """Mark a Do-next step done: the profile is edited as if the move were
    made (the source sold down, the debt paid, the new holdings added, or the
    monthly split rewritten) and the previous profile is kept for one undo.
    409 when the step is no longer on the list or cannot be done here."""
    gate = _admin_gate(request)
    if gate:
        return gate
    import allocation as A
    import capital as K
    import capital_view as V
    from datetime import datetime, timezone
    from database import get_capital_profile, save_capital_profile
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Expected a JSON body."}, status_code=400)
    step_id = str((body or {}).get("id") or "") if isinstance(body, dict) else ""
    if not step_id:
        return JSONResponse({"error": "Which step? Send its id."}, status_code=400)
    saved = {k: v for k, v in (get_capital_profile() or {}).items() if not k.startswith("_")}
    board = await asyncio.to_thread(K.build, saved)
    # the step edits the lines the board measured: at today's prices
    live = {**saved, "holdings": board["profile"].get("holdings", saved.get("holdings"))}
    try:
        prof, step = await asyncio.to_thread(V.apply_step, live, board, date.today(), step_id)
    except A.CannotApply as e:
        return JSONResponse({"error": str(e)}, status_code=409)
    if not save_capital_profile({k: v for k, v in saved.items() if k != "last_step"}, owner="owner:undo"):
        return JSONResponse({"error": "Could not save — the database is unavailable."}, status_code=503)
    prof["last_step"] = {"title": step["title"], "impact": step["impact"],
                         "at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    if not save_capital_profile(prof):
        return JSONResponse({"error": "Could not save — the database is unavailable."}, status_code=503)
    return JSONResponse({"ok": True, "title": step["title"]})


@app.post("/api/capital/move")
async def capital_move(request: Request):
    """Carry out a what-if move ({"move": {"line", "amount", "dest"}}): the
    move is re-checked against the profile as saved, then applied the way a
    Do-next step is, with the same one-level undo."""
    gate = _admin_gate(request)
    if gate:
        return gate
    import allocation as A
    import capital as K
    import whatif as W
    from datetime import datetime, timezone
    from database import get_capital_profile, save_capital_profile
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Expected a JSON body."}, status_code=400)
    move = (body or {}).get("move") if isinstance(body, dict) else None
    if not isinstance(move, dict):
        return JSONResponse({"error": "Which move? Send it as {\"move\": {...}}."}, status_code=400)
    saved = {k: v for k, v in (get_capital_profile() or {}).items() if not k.startswith("_")}
    board = await asyncio.to_thread(K.build, saved)
    r = W.evaluate(W.kit(board), move)
    if not r.get("ok"):
        return JSONResponse({"error": r.get("error")}, status_code=409)
    live = {**saved, "holdings": board["profile"].get("holdings", saved.get("holdings"))}
    try:
        prof = A.apply_moves(live, [W.as_move(r)], board)
    except A.CannotApply as e:
        return JSONResponse({"error": str(e)}, status_code=409)
    if not save_capital_profile({k: v for k, v in saved.items() if k != "last_step"}, owner="owner:undo"):
        return JSONResponse({"error": "Could not save — the database is unavailable."}, status_code=503)
    prof["last_step"] = {"title": "What if: " + W.summary(r), "impact": round(r["per_year"], 2),
                         "at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    if not save_capital_profile(prof):
        return JSONResponse({"error": "Could not save — the database is unavailable."}, status_code=503)
    return JSONResponse({"ok": True, "title": W.summary(r)})


async def _import_body(request: Request):
    try:
        body = await request.json()
    except Exception:
        return None, JSONResponse({"error": "Expected a JSON body."}, status_code=400)
    if not isinstance(body, dict) or not isinstance(body.get("csv"), str):
        return None, JSONResponse({"error": "Send the export as {\"csv\": \"...\"}."}, status_code=400)
    return body, None


@app.post("/api/capital/import/preview")
async def capital_import_preview(request: Request):
    """Read a broker's positions export ({"csv", "filename"}) and say what a
    sync would write — each account, its positions, the check against the
    broker's total, a margin loan, and the lines already typed for those
    accounts. Nothing is saved."""
    gate = _admin_gate(request)
    if gate:
        return gate
    import positions as P
    from database import get_capital_profile
    body, err = await _import_body(request)
    if err:
        return err
    try:
        parsed = P.parse_export(body["csv"], str(body.get("filename") or "")[:200])
    except P.CannotImport as e:
        return JSONResponse({"error": str(e)}, status_code=400)
    saved = get_capital_profile() or {}
    text = saved.get("holdings") or ""
    before = {b["key"]: P.describe_block(b) for b in P.blocks_in(text)}
    debts = {d.get("name"): d for d in saved.get("debts") or [] if isinstance(d, dict)}
    for a in parsed["accounts"]:
        as_named = a
        if a.get("unnamed"):
            # the file does not name it: suggest the name its last export was synced under
            mine = [d for k, d in before.items() if k.startswith(a["broker"].lower() + "-") and not k[-1].isdigit()]
            a["name"] = mine[0]["label"].split(" · ", 1)[-1] if len(mine) == 1 else P.UNNAMED
            as_named = P.named(a, a["name"])
        a["synced_before"] = before.get(as_named["key"])
        old = debts.get(P.margin_debt_name(as_named))
        a["margin_apr"] = old.get("apr") if old else None
    return JSONResponse({**parsed, "overlaps": P.overlaps(text, parsed["accounts"]),
                         "synced": {k: d["as_of"] for k, d in before.items()},
                         "debt_aprs": {d["name"]: d.get("apr") for d in debts.values() if " margin " in str(d.get("name"))}})


@app.post("/api/capital/import/apply")
async def capital_import_apply(request: Request):
    """Sync the chosen accounts of an export into the holdings (the export is
    read again here, not taken from the page), with the same one-level undo
    as a step marked done."""
    gate = _admin_gate(request)
    if gate:
        return gate
    import positions as P
    from datetime import datetime, timezone
    from database import get_capital_profile, save_capital_profile
    body, err = await _import_body(request)
    if err:
        return err
    saved = {k: v for k, v in (get_capital_profile() or {}).items() if not k.startswith("_")}
    try:
        parsed = P.parse_export(body["csv"], str(body.get("filename") or "")[:200])
        prof, title = P.apply_import(saved, parsed, body)
    except P.CannotImport as e:
        return JSONResponse({"error": str(e)}, status_code=400)
    if not save_capital_profile({k: v for k, v in saved.items() if k != "last_step"}, owner="owner:undo"):
        return JSONResponse({"error": "Could not save — the database is unavailable."}, status_code=503)
    prof["last_step"] = {"title": title, "impact": None, "at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    if not save_capital_profile(prof):
        return JSONResponse({"error": "Could not save — the database is unavailable."}, status_code=503)
    return JSONResponse({"ok": True, "title": title})


@app.post("/api/capital/undo")
async def capital_step_undo(request: Request):
    """Put back the profile as it was before the last step marked done."""
    gate = _admin_gate(request)
    if gate:
        return gate
    from database import get_capital_profile, save_capital_profile
    prev = {k: v for k, v in (get_capital_profile("owner:undo") or {}).items() if not k.startswith("_")}
    if not prev:
        return JSONResponse({"error": "Nothing to undo."}, status_code=404)
    prev.pop("last_step", None)
    if not (save_capital_profile(prev) and save_capital_profile({}, owner="owner:undo")):
        return JSONResponse({"error": "Could not save — the database is unavailable."}, status_code=503)
    return JSONResponse({"ok": True})


@app.get("/compounders")
async def compounders_page(request: Request):
    """The 14%/yr long-term screen: quality gates (ROIC, consistency,
    cash conversion, debt, capex) + a transparent expected-return
    decomposition (growth + buybacks + dividends ± valuation drift).
    Universe self-discovers from SEC EDGAR (≥ $1B revenue, incl. ADRs);
    data built monthly by refresh_compounders.yml."""
    from compounders import score, summary, data_source_label, ROIC_MIN, TARGET, FCF_CONV_CAP
    rows = score()
    return templates.TemplateResponse("compounders.html", {
        "request":          request,
        "rows":             rows,
        "compounder_cards": [r for r in rows if r["status"] == "COMPOUNDER"][:12],
        "summary":          summary(rows),
        "data_source":      data_source_label(),
        "roic_min":         ROIC_MIN,
        "target":           TARGET,
        "C_CAP":            FCF_CONV_CAP,
    })


@app.get("/holt")
async def holt_page(request: Request):
    """The starting multiple is a choice; the growth is a bet.

    UBS HOLT's grid of median 5-year excess returns, cut by starting
    multiple against 5-year FORWARD sales growth. The column axis cannot
    be screened on — it is growth that has not happened — so this page
    treats it as a probability measured from how trailing growth actually
    rolled forward in our own universe, and scores each company as HOLT's
    row weighted by that distribution.

    The finding the page exists to carry: every cell of the 50x+ row is
    negative even after a company delivers 20%+ growth. You cannot
    reliably choose the column. You always choose the row.
    """
    import holt as H
    from compounders import _load
    data = _load() or {}
    rows = [{**m, "ticker": t} for t, m in (data.get("tickers") or {}).items()
            if isinstance(m, dict)]

    # Transitions measured from THIS universe rather than the baked
    # default, so the probabilities age with the data instead of
    # freezing at whatever the file said the day it was written.
    pairs = []
    for r in rows:
        e = H.early_cagr(r.get("revenue_last"), r.get("rev_cagr5"),
                         r.get("rev_cagr10"))
        if e is not None and r.get("rev_cagr5") is not None:
            pairs.append((e, r["rev_cagr5"]))
    live = H.transition_matrix(pairs)
    transitions = live if len(live) == len(H.GROWTH_BANDS) else H.DEFAULT_TRANSITIONS

    cap = (request.query_params.get("cap") or "").strip()
    if cap not in H.MULTIPLE_BANDS:
        cap = None
    show_flagged = request.query_params.get("flagged") == "1"
    res = H.rank(rows, transitions, require_clean=not show_flagged, max_band=cap)

    # The weighted grid — HOLT's table as it reads once the column is a
    # bet rather than a fact. This is the page's centrepiece.
    weighted = {}
    for mb in H.MULTIPLE_BANDS:
        for gb in H.GROWTH_BANDS:
            ev = H.expected_excess(mb, gb, transitions)
            if ev is not None:
                weighted[f"{mb}|{gb}"] = round(ev, 1)

    return templates.TemplateResponse("holt.html", {
        "request": request,
        "rows": res["rows"][:200],
        "counts": res["counts"],
        # Every data fault, not the first 40 refusals filtered afterwards —
        # that kept 1 of 45. Rows with no multiple at all are counted by
        # reason instead; they are facts about the company, not faults.
        "refused": res["refused"],
        "unmeasured": res["unmeasured"],
        "grid": H.GRID,
        "weighted": weighted,
        "census": H.grid_census(res["clean"]),
        "multiple_bands": H.MULTIPLE_BANDS,
        "growth_bands": H.GROWTH_BANDS,
        "growth_labels": H.GROWTH_LABELS,
        "transitions": transitions,
        "transitions_live": len(live) == len(H.GROWTH_BANDS),
        "transition_n": len(pairs),
        "proxy": H.MULTIPLE_PROXY,
        "cap": cap,
        "show_flagged": show_flagged,
        "as_of": data.get("as_of"),
    })


@app.get("/schloss")
async def schloss_page(request: Request):
    """Walter Schloss's method, run off SEC filings alone.

    Tangible book, net current assets, debt, filing longevity, dividends
    (paying is a governance check; a CUT is the entry signal) and the
    stock-comp/dilution read on whether management overpays itself.

    Deliberately price-free. The cheapness half — 20% discount to book,
    two-thirds of NCAV, the 52-week low he started from — needs a market
    feed and reports UNKNOWN without one rather than guessing. Data built
    monthly by refresh-schloss.yml from the EDGAR frames API, which covers
    every filer in one call per concept, so this screen reaches the small
    and unloved end of the market that a revenue floor would delete."""
    import schloss as SC
    payload = SC.load()
    # HAND-ENTERED QUOTES, merged on read. 894 of 5,726 rows arrive with no
    # price because no free feed carries them — every OTC name, and every
    # dual-class ticker including Berkshire. The balance sheet for those is
    # fully screened; only the cheapness half is blank. A price typed in
    # here fills it using the same arithmetic the build uses, and the file
    # on disk stays exactly what the Action produced.
    hand_prices = {}
    try:
        from database import get_all_prices
        hand_prices = get_all_prices() or {}
    except Exception:
        logger.exception("schloss: could not read hand-entered prices")
    if hand_prices:
        for row in payload.get("rows") or []:
            q = hand_prices.get((row.get("ticker") or "").upper())
            if not q:
                continue
            shares = SC.shares_from_row(row)
            if shares is None:
                row["hand_price_note"] = (
                    f"${q['price']:g} entered, but no share count is "
                    f"recoverable from this row, so no market cap follows")
                continue
            row.update(SC.reprice(row, q["price"] * shares))
            row["hand_price"] = q["price"]
            row["hand_price_shares"] = shares
            row["hand_price_at"] = q.get("entered_at")
    return templates.TemplateResponse("schloss.html", {
        "request":     request,
        "payload":     payload,
        "rows":        payload.get("rows") or [],
        "census":      payload.get("census") or {},
        "can_edit":    _check_admin_token(request),
        "lists":       SC.board(payload),
        "data_source": SC.data_source_label(payload),
        "S":           SC,
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


@app.get("/api/stock/search")
async def api_stock_search(q: str = ""):
    """Ticker search for the type-ahead boxes: US stocks, ETFs and mutual
    funds by ticker or name (Yahoo, then Nasdaq, then the SEC's list)."""
    from stock_lookup import search_tickers
    return JSONResponse(await asyncio.to_thread(search_tickers, q))


@app.get("/api/stock/{ticker}/price")
async def api_stock_price(ticker: str):
    """The latest price for one ticker — the light version of /quote, for a
    price beside a holding."""
    from stock_lookup import get_price
    return JSONResponse(await asyncio.to_thread(get_price, ticker))


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


_ZIP_LAYER_CACHE: dict = {}


def _zip_layer(name: str) -> dict:
    """data/<name>.json (zip_climate, zip_hazards), cached until the file changes.

    Returns {} when the file is absent or unreadable, which the finder reads
    as "this source has no data" — the filters that need it are then listed
    as unavailable rather than offered over nothing.

    MF_ZIP_LAYER_DIR (unset in production) points at another folder: the page
    test serves a copy with one state removed, since no real state lacks
    these layers any more.
    """
    base = os.environ.get("MF_ZIP_LAYER_DIR") or Path(__file__).resolve().parent / "data"
    p = Path(base) / f"{name}.json"
    try:
        mtime = p.stat().st_mtime
    except OSError:
        return {}
    hit = _ZIP_LAYER_CACHE.get(name)
    if hit and hit[0] == mtime:
        return hit[1]
    try:
        payload = json.loads(p.read_text())
    except (OSError, ValueError):
        payload = {}
    _ZIP_LAYER_CACHE[name] = (mtime, payload)
    return payload


@app.get("/multifamily")
async def multifamily_page(
    request: Request,
    state: str = "OH",
    max_price: str = "550000",
    down_pct: str = "3.5",
    units: str = "2",
    rent_unit: str = "",
    safetier: str = "safe",
    unknown: str = "0",
    check_price: str = "",
    check_rent: str = "",
    check_income: str = "",
    check_debts: str = "",
    # ── finder: filters (hard) ──
    area: list[str] | None = Query(None),
    min_income: str = "",
    min_degree: str = "",
    max_cost: str = "",
    min_cap: str = "",
    min_trend: str = "",
    min_renter: str = "",
    min_multi: str = "",
    min_young: str = "",
    min_2_4: str = "",
    min_winter: str = "",
    max_summer: str = "",
    max_snow: str = "",
    max_flood: str = "",
    max_fire: str = "",
    max_wind: str = "",
    max_quake: str = "",
    # ── finder: priorities (soft) ──
    use_preset: str = "",
    w_cashflow: str = "",
    w_yield: str = "",
    w_growth: str = "",
    w_income: str = "",
    w_education: str = "",
):
    """Multifamily ZIP finder, tuned for first-time FHA owner-occupants.

    YOU STATE WHAT YOU WANT; THE PAGE SHOWS WHICH ZIPS HAVE IT. Filters are
    hard yes/no (income, degree share, area type, cost after rent, cap rate,
    price trend), priorities are soft weights among what survives. The logic
    lives in zip_finder.py, which is pure and tested; this route fetches the
    rows, does the FHA underwriting, and wires the two together.

    EVERY REMOVAL IS ACCOUNTED FOR. The old query applied population, rent
    and budget filters in one WHERE clause, so a state fell from 1,017 ZIPs
    to 280 before the page said anything — two thirds of Ohio vanished for
    having no measured Zillow rent, and the page never mentioned it. Each of
    those is now a named step in the funnel.

    SAFETY: this board ranks where a family would LIVE, so it runs the same
    real-FBI safety gate as the strict screen (safety.py) — never the
    zips.db crime_index, which is density+income+education and correlates
    -0.76 with median income. An unmeasured city never passes as safe.

    NATIONAL OR ONE STATE. state=ALL runs the same funnel over every ZIP in
    the country, pricing each row with ITS OWN state's tax and insurance
    rates, safety record and structural flags. The scenario card and the
    listing checker price one building with one state's rates, so in the
    national view they ask for a state instead of guessing one.

    WEATHER AND HAZARDS come from data/zip_climate.json (NOAA normals) and
    data/zip_hazards.json (FEMA expected building loss), joined by ZIP. Their
    filters are offered only where those files have figures — see zip_env.py
    and the two refresh scripts for how the figures are made.

    Default frame: ~3.5% FHA cash, a 2-4 unit under $550k, house-hacking
    (live in one unit, rent the others).
    """
    import sqlite3
    from data_providers import MORTGAGE_30Y_RATE
    import safety as SF
    import househack as HH
    import rent_ladder as RL
    import zip_finder as ZF
    safetier = SF.valid_tier(safetier)
    allow_unknown_safety = _qnum(unknown) > 0
    state = (state or "OH").upper()
    national = state == "ALL"
    place = "the U.S." if national else state
    # Clamp inputs so a wonky URL param can't blow up the math. Parsed
    # tolerantly (_qnum) — an emptied number field submits "" and must
    # fall back to the default, never 422.
    max_price = max(50_000, min(2_000_000, int(_qnum(max_price, 550_000))))
    down_pct = max(0.0, min(50.0, _qnum(down_pct, 3.5)))
    units = max(2, min(4, int(_qnum(units, 2))))
    rent_unit_n = max(0.0, min(50_000.0, _qnum(rent_unit)))
    # Deal checker: a specific listing at a specific ask with the rents
    # the seller claims. This is the only place on the page where the two
    # weakest board inputs — an ESTIMATED building price and an often
    # IMPUTED rent — are replaced by numbers the user actually has.
    chk_price = max(0.0, min(5_000_000.0, _qnum(check_price)))
    chk_rent = max(0.0, min(50_000.0, _qnum(check_rent)))
    chk_income = max(0.0, min(10_000_000.0, _qnum(check_income)))
    chk_debts = max(0.0, min(50_000.0, _qnum(check_debts)))

    filters = ZF.parse_filters({
        "area": area or [], "min_income": min_income, "min_degree": min_degree,
        "max_cost": max_cost, "min_cap": min_cap, "min_trend": min_trend,
        "min_renter": min_renter, "min_multi": min_multi,
        "min_young": min_young, "min_2_4": min_2_4,
        "min_winter": min_winter, "max_summer": max_summer, "max_snow": max_snow,
        "max_flood": max_flood, "max_fire": max_fire, "max_wind": max_wind,
        "max_quake": max_quake})
    weights, preset = ZF.parse_priorities(use_preset, {
        "w_cashflow": w_cashflow, "w_yield": w_yield, "w_growth": w_growth,
        "w_income": w_income, "w_education": w_education})

    # Everything the form needs to re-render the user's choices, and every
    # param the deal-checker form must carry so running a listing does not
    # silently reset the filters above it.
    finder_ctx = {
        "zf": ZF, "filters": filters, "weights": weights, "preset": preset,
        "carry": ([("area", a) for a in filters["area"]]
                  + [(f["key"], f"{filters[f['key']]:g}") for f in ZF.active_filters(filters)]
                  + [(f"w_{k}", str(weights[k])) for k in ZF.PRIORITY_KEYS]),
    }

    db_path = Path(__file__).resolve().parent / "data" / "zips.db"
    if not db_path.exists():
        return templates.TemplateResponse("multifamily.html", {
            "request": request, "rows": [], "state": state, "states": [],
            "has_mf_data": False, "data_pending": True,
            "max_price": max_price, "down_pct": down_pct, "units": units,
            "rent_unit": (int(rent_unit_n) if rent_unit_n > 0 else ""),
            "hh_scenario": None, "rent_source": None,
            # The template dereferences these unconditionally; a bare
            # Undefined raises in Jinja, so a missing database has to
            # degrade to an empty page, not a 500.
            "safetier": safetier, "allow_unknown_safety": allow_unknown_safety,
            "safety_coverage": SF.coverage(), "gate": None, "deal": None,
            "sample_piti": None, "vs_rent": None, "qualify": None,
            "selfsuff": None, "reserves": None, "rent_observed_pct": 0,
            "check_price": "", "check_rent": "", "check_income": "",
            "check_debts": "", "state_struct": None, "has_history": False,
            "suggest": None, "unit_factor": 1.0, "hv_ceiling": 0,
            "mortgage_rate": MORTGAGE_30Y_RATE,
            "funnel": None, "emptied_by": None, "offered": [], "n_total": 0,
            "score_info": None, "national": national, "place": place,
            "climate_meta": None, "hazards_meta": None,
            "rent_tiers": RL.TIER_BY_KEY, "rent_mix": {},
            **finder_ctx,
        })
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    states = [r[0] for r in cur.execute(
        "select distinct state from zips where state is not null and state != '' order by state"
    ).fetchall()]
    cols = {r[1] for r in cur.execute("PRAGMA table_info(zips)").fetchall()}

    # Affordability. The user's cap is what they'd pay for the BUILDING,
    # and a 2-4 unit does not cost the single-family median — so the SFR
    # ceiling is the cap divided by the unit price factor, not the cap
    # itself. The 20% headroom stays: a median is a midpoint, so there is
    # cheaper stock in the ZIP and the user may stretch a little.
    unit_factor = HH.UNIT_PRICE_FACTOR.get(units, 1.55)
    hv_ceiling = int(max_price * 1.2 / unit_factor)

    has_history = "history_zhvi" in cols
    want = ("zip", "state", "name", "neighborhood", "lat", "lng", "population",
            "population_density", "median_home_value", "median_rent_monthly",
            "cap_rate_pct", "median_household_income", "pct_bachelors",
            "pct_renter_occupied", "pct_multi_unit", "pct_rent_burdened",
            "pct_age_25_34", "pct_2_4_units", "history_zhvi", "rent_tier")
    select_cols = ", ".join(c if c in cols else f"NULL as {c}" for c in want)
    # EVERY row in scope, deliberately. The filters that used to live in
    # this WHERE clause now run through the funnel so each one is counted.
    if national:
        all_rows = [dict(zip(want, r)) for r in cur.execute(
            f"select {select_cols} from zips where state is not null and state != ''").fetchall()]
    else:
        all_rows = [dict(zip(want, r)) for r in cur.execute(
            f"select {select_cols} from zips where state = ?", (state,)).fetchall()]
    conn.close()

    # Weather and hazards, joined by ZIP. Missing stays missing (None), which
    # no filter lets pass.
    climate, hazards = _zip_layer("zip_climate"), _zip_layer("zip_hazards")
    c_zips, h_zips = climate.get("zips") or {}, hazards.get("zips") or {}
    for r in all_rows:
        c = c_zips.get(r["zip"]) or {}
        r["winter_low"], r["summer_high"], r["snow_in"] = c.get("wl"), c.get("sh"), c.get("sn")
        r["days_90"], r["days_32"] = c.get("d90"), c.get("d32")
        r["temp_km"], r["snow_km"] = c.get("tk"), c.get("sk")
        # Hazards: expected yearly building damage, $ per $100k of building.
        # The total needs all four; a partial sum would read as a whole one.
        h = h_zips.get(r["zip"]) or {}
        for group, key in (("flood", "fl"), ("wildfire", "wf"), ("wind", "wd"), ("quake", "eq")):
            r[f"{group}_rate"] = h.get(key)
        parts = [h.get(k) for k in ("fl", "wf", "wd", "eq")]
        r["hazard_total"] = round(sum(parts), 2) if None not in parts else None

    # Sources that actually carry data FOR THE ROWS IN SCOPE. A filter over an
    # empty source would mark every row NO DATA and empty the board while
    # looking like a strict choice, so filters are offered only where their
    # source has figures — a state a source doesn't reach gets no filter for
    # it rather than a board emptied by one.
    with_data = set()
    for c in ("pct_renter_occupied", "pct_multi_unit", "pct_rent_burdened",
              "pct_age_25_34", "pct_2_4_units"):
        if any(r.get(c) is not None for r in all_rows):
            with_data.add(c)
    if any(r["winter_low"] is not None for r in all_rows):
        with_data.add("zip_climate")
    if any(r["flood_rate"] is not None for r in all_rows):
        with_data.add("zip_hazards")
    has_mf_data = "pct_renter_occupied" in with_data
    offered = [f for f in ZF.FILTERS if ZF.available(f, with_data)]
    # A filter that is not offered cannot be active, even if a URL asks.
    for f in ZF.FILTERS:
        if f not in offered:
            filters[f["key"]] = None

    # ── The funnel: every step from "ZIPs in scope" to "on the board" ──
    funnel = ZF.Funnel("U.S. ZIPs" if national else f"{state} ZIPs", all_rows)
    funnel.step("population", "fewer than 1,500 residents — too small to price reliably",
                keep=lambda r: (r["population"] or 0) >= 1500,
                no_data=lambda r: r["population"] is None)
    funnel.step("rent", "no measured rent (Zillow or HUD), so it can't be underwritten",
                keep=lambda r: (r["median_home_value"] is not None
                                and r["median_rent_monthly"] is not None
                                and r["cap_rate_pct"] is not None),
                # Every removal here is a gap in the data, not a finding.
                no_data=lambda r: True)
    funnel.step("budget",
                f"over your budget — single-family median above ${hv_ceiling:,} "
                f"(a {units}-unit runs ~{unit_factor}× that)",
                keep=lambda r: r["median_home_value"] <= hv_ceiling)

    # ── Per-row FHA house-hack math, for the ZIPs still in play ──────
    # Purchase = min(single-family median × the unit factor, your cap).
    # Net cost = PITI − (units − 1) × median rent. Negative means the
    # renters pay more than your full housing cost — you live free + cash.
    from structural import (trajectory_from_history, durable_cap_rate,
                            state_structural, apply_trajectory_veto,
                            TRAJECTORY_BADGES)
    # Structural flags are per STATE. One lookup per state in scope, not per row.
    _struct: dict = {}

    def struct_for(st):
        if st not in _struct:
            _struct[st] = state_structural(st)
        return _struct[st]

    state_struct = None if national else struct_for(state)

    for r in funnel.rows:
        hv, rent = r["median_home_value"], r["median_rent_monthly"]
        # Value trajectory from the ZIP's 60-month ZHVI history — the
        # structural lens: a great level score with a declining series
        # gets vetoed below, not averaged away.
        traj = None
        if r["history_zhvi"]:
            try:
                traj = trajectory_from_history(json.loads(r["history_zhvi"]))
            except (ValueError, TypeError):
                traj = None
        r["traj"] = traj
        r["traj_badge"] = TRAJECTORY_BADGES.get(traj["label"]) if traj else None
        r["trend_3yr"] = traj["cagr_3yr_pct"] if traj else None
        st = r["state"]
        # One source for the row's state flags: the veto uses it and the page
        # shows it, so a wrong state's flags are visible rather than silent.
        r["state_flags"] = len(struct_for(st)["flags"])
        durable = durable_cap_rate(r["cap_rate_pct"], hv, st)
        r["durable_detail"] = durable
        r["durable_cap_pct"] = durable["durable_cap_pct"] if durable else None
        r["area_type"] = ZF.area_type(r["population_density"])
        r["density_sqmi"] = ZF.per_sq_mi(r["population_density"])
        r["building_price"] = round(min(hv * unit_factor, max_price))
        r["safety"] = SF.zip_safety(r["name"], st)
        # A rent the ladder wrote carries its source, and that is proof it
        # was measured. The value/17/12 arithmetic test is only for rows
        # written before rent_tier existed: applied to measured rents it
        # starred 392 real Zillow figures as "imputed" because they
        # happened to land within 2% of home value ÷ 204.
        r["rent_observed"] = (r.get("rent_tier") in RL.TIER_BY_KEY
                              or HH.rent_is_observed(rent, hv))
        r["rent_strains_income"] = RL.hud_rent_strains_income(
            rent, r.get("rent_tier"), r["median_household_income"])

        # Price and insure the row exactly as the scenario card and the
        # deal checker do. When only the card applied the unit factors,
        # a ZIP could show +$932 cash flow and a passing FHA badge in the
        # table while the same building priced as a triplex was -$968 and
        # failed the funding gate — a $1,900/mo swing on one screen.
        purchase = min(hv * unit_factor, max_price)
        piti = HH.apply_unit_insurance(
            _fha_piti(purchase, st, MORTGAGE_30Y_RATE, down_pct=down_pct), units)
        if not piti:
            r["house_hack_net"] = r["house_hack_stressed"] = r["fha_self_suff"] = None
            continue
        expected_rent = (units - 1) * rent
        r["house_hack_net"] = round(piti["piti"] - expected_rent)
        # Stress test: same deal if rents come in 10% light, insurance
        # reprices +30%, and you eat one vacant month per year. If THIS
        # number still cash-flows, the deal survives a structural shift.
        stressed_piti = piti["piti"] + 0.30 * piti["monthly_ins"]
        stressed_rent = expected_rent * 0.90 * (11 / 12)
        r["house_hack_stressed"] = round(stressed_piti - stressed_rent)
        # FHA self-sufficiency test (3-4 unit only): 75% of *total*
        # rents (all units, including owner-occupied) must cover PITI.
        r["fha_self_suff"] = (0.75 * units * rent >= piti["piti"]) if units >= 3 else None

    funnel.step("priced", "couldn't be priced (no tax or insurance rate for the state)",
                keep=lambda r: r["house_hack_net"] is not None,
                no_data=lambda r: True)

    # Safety gate: the same gate and the same fail-closed rule the strict
    # screen uses. A yield ranking with no safety gate is a ranking of
    # distress. "We measured it and it isn't safe" and "nobody published a
    # figure" are different problems, and only the second is ours to fix —
    # so the funnel counts them apart, the same way it does for every filter.
    # SF.TIERS rows are (key, lo, hi, LABEL, description).
    tier_row = next((t for t in SF.TIERS if t[0] == safetier), None)
    tier_label = (f"{tier_row[3].lower()}, under {tier_row[2]:,.0f} per 100k"
                  if tier_row and math.isfinite(tier_row[2]) else "none set")
    funnel.step(
        "safety", f"violent crime above your bar ({tier_label})",
        keep=lambda r: SF.passes(r["safety"], safetier, allow_unknown_safety),
        no_data=lambda r: r["safety"]["tier"] == "unknown", kind="safety")
    s = funnel.stages[-1]
    gate = {"before": s["before"], "after": s["after"],
            "unverified": s["no_data"], "above_tier": s["removed"] - s["no_data"]}

    # Your filters, one funnel step each.
    rows = ZF.apply_filters(funnel, filters)
    emptied_by = funnel.emptied_by()

    # ── Rank what survived by what you said matters ─────────────────
    score_info = ZF.score(rows, weights)
    for r in rows:
        r["vetoed"] = False
        if r["mf_score"] is not None:
            # Trajectory veto: a declining/decelerating ZIP can't ride a
            # cheap-level score to the top of the table. Vetoed rows are
            # marked so the UI shows *why* the score is capped.
            r["mf_score"], r["vetoed"] = apply_trajectory_veto(
                r["mf_score"], r["traj"]["label"] if r["traj"] else None, r["state_flags"])
    # Verified-safe rows by score, then rows whose safety nobody measured —
    # shown only when asked for, and never ranked above a measured one.
    rows = ZF.board_order(rows)

    # An empty board should name the binding constraint instead of just
    # being empty. When it is the budget or the safety bar, find the
    # cheapest ZIP that DOES clear the safety bar and quote the cap it would
    # take — so the user can tell "too strict" apart from "nothing exists".
    # When it is one of their own filters, raising the budget would not
    # help, so no budget suggestion is made.
    suggest = None
    if not rows and emptied_by and emptied_by["key"] in ("budget", "safety"):
        c2 = sqlite3.connect(str(db_path))
        scope_sql, scope_args = ("", ()) if national else ("state = ? and ", (state,))
        cand = c2.execute(
            "select zip, name, median_home_value, state from zips "
            f"where {scope_sql}population >= 1500 "
            "and median_home_value is not null and median_rent_monthly is not null "
            "order by median_home_value asc limit 4000", scope_args).fetchall()
        c2.close()
        for z2, n2, hv2, st2 in cand:
            if hv2 and SF.passes(SF.zip_safety(n2, st2), safetier, allow_unknown_safety):
                bp = hv2 * unit_factor
                # Round the suggested cap UP: rounding to the nearest
                # $10k can land just under the threshold, producing a
                # "retry at $550,000" link on a page already at $550,000.
                suggest = {"zip": z2, "name": n2, "sfr": round(hv2),
                           "building": round(bp),
                           "cap_needed": int(math.ceil(bp / 1.2 / 10_000) * 10_000)}
                break

    # ── Scenario: price the PROPERTIES RETURNED, not the filter cap ──
    # The cap is a filter. Pricing the scenario AT the cap described a
    # property that does not exist in these ZIPs ($350k cap over stock
    # worth $75-160k), overstating PITI, cash and required income on
    # every row. Anchor on the median of the qualifying set instead, and
    # adjust the single-family median up to a 2-4 unit building price.
    import statistics
    from value_add import house_hack_scenario
    shown = rows[:100]
    sfr_medians = [r["median_home_value"] for r in shown if r["median_home_value"]]
    # No qualifying ZIPs means no building to price. Falling back to the
    # filter cap produced a card quoting PITI, required income and cash to
    # close for a $550k property on a page that had just said nothing in
    # the state qualifies — while the card's own footnote claimed it was
    # priced from "the ZIPs actually listed below".
    sfr_anchor = statistics.median(sfr_medians) if sfr_medians else None
    bld = HH.est_building_price(sfr_anchor, units) if sfr_anchor else None
    # One building, one state's tax and insurance rates — so no scenario in
    # the national view. Averaging rates across states would price a
    # building that exists nowhere; the page asks for a state instead.
    scenario_price = min(bld["estimated_price"], max_price) if bld and not national else None
    price_capped = bool(bld and bld["estimated_price"] > max_price)

    # An owner-occupied 2-4 unit is not an SFR risk: more units, more
    # liability, higher rebuild. Same helper the table rows use.
    sample_piti = (HH.apply_unit_insurance(
        _fha_piti(scenario_price, state, MORTGAGE_30Y_RATE, down_pct=down_pct), units)
        if scenario_price else None)

    # ── Rent basis: OBSERVED rents only ──
    # A rent imputed as value/17/12 is a restatement of home value, so a
    # median built from those would quote circular arithmetic back at the
    # user as "market rent". Use observed rents whenever any exist, and
    # name whichever basis is used.
    observed = [r["median_rent_monthly"] for r in shown if r["rent_observed"]]
    all_rents = [r["median_rent_monthly"] for r in shown if r["median_rent_monthly"]]
    THIN_RENT_SAMPLE = 5
    obs_rent = statistics.median(observed) if observed else None
    eff_rent = (rent_unit_n if rent_unit_n > 0
                else (obs_rent if obs_rent is not None
                      else (statistics.median(all_rents) if all_rents else 0)))
    rent_thin = 0 < len(observed) < THIN_RENT_SAMPLE
    rent_basis = ("input" if rent_unit_n > 0 else
                  "observed" if obs_rent is not None else
                  "imputed" if all_rents else "none")
    rent_source = {
        "input": "your input",
        "observed": (f"median of {len(observed)} ZIP{'s' if len(observed) != 1 else ''} "
                     f"with measured rents (Zillow or HUD), of {len(all_rents)} shown"
                     + (" — a thin sample, treat as indicative" if rent_thin else "")),
        "imputed": (f"median of {len(all_rents)} shown ZIPs — every one IMPUTED "
                    "(value÷17÷12), so this restates home values, not market rent"),
        "none": None,
    }[rent_basis]
    rent_observed_pct = round(len(observed) / len(all_rents) * 100) if all_rents else 0
    # Which source answered each row's rent, for the legend. A board that
    # mixes Zillow asking rents with HUD's voucher benchmark says so.
    rent_mix: dict = {}
    for r in shown:
        if r["median_rent_monthly"]:
            k = r.get("rent_tier")
            rent_mix[k] = rent_mix.get(k, 0) + 1

    hh_scenario = house_hack_scenario(sample_piti, units, eff_rent) if sample_piti else None
    piti_m = sample_piti["piti"] if sample_piti else 0.0
    vs_rent = HH.vs_renting(piti_m, units, eff_rent) if sample_piti and eff_rent else None
    qualify = HH.income_to_qualify(piti_m, units, eff_rent) if sample_piti else None
    selfsuff = HH.fha_self_sufficiency(piti_m, units, eff_rent) if sample_piti else None
    reserves = (HH.reserves_needed(piti_m, sample_piti["closing_est"],
                                   sample_piti["down_cash"], units)
                if sample_piti else None)

    # ── Deal checker: one real listing, real rents ──
    # Everything above is a market-level estimate. This runs the same
    # underwriting on numbers the user typed off an actual listing, so
    # the answer stops depending on our SFR-to-building price factor or
    # on whether this ZIP happens to have an observed rent series.
    deal = None
    if chk_price > 0 and chk_rent > 0 and not national:
        dp = _fha_piti(chk_price, state, MORTGAGE_30Y_RATE, down_pct=down_pct)
        if dp:
            HH.apply_unit_insurance(dp, units)
            deal = HH.check_deal(
                dp["piti"], units, chk_rent, chk_price,
                insurance_monthly=dp["monthly_ins"],
                closing=dp["closing_est"], down=dp["down_cash"],
                your_income=(chk_income or None), other_debts=chk_debts)
            deal["piti"] = dp

    return templates.TemplateResponse("multifamily.html", {
        "request": request,
        "rows": shown, "n_total": len(rows),
        "state": state, "states": states,
        "max_price": max_price, "down_pct": down_pct, "units": units,
        "rent_unit": (int(rent_unit_n) if rent_unit_n > 0 else ""),
        "hh_scenario": hh_scenario, "rent_source": rent_source,
        "sample_piti": sample_piti,
        "scenario_price": scenario_price, "price_basis": bld,
        "price_capped": price_capped,
        "sfr_anchor": (round(sfr_anchor) if sfr_anchor else None),
        "vs_rent": vs_rent, "qualify": qualify, "selfsuff": selfsuff,
        "reserves": reserves, "rent_observed_pct": rent_observed_pct,
        "rent_basis": rent_basis, "rent_thin": rent_thin,
        "n_observed": len(observed), "n_rents": len(all_rents),
        "deal": deal,
        "check_price": (int(chk_price) if chk_price > 0 else ""),
        "check_rent": (int(chk_rent) if chk_rent > 0 else ""),
        "check_income": (int(chk_income) if chk_income > 0 else ""),
        "check_debts": (int(chk_debts) if chk_debts > 0 else ""),
        "safetier": safetier, "allow_unknown_safety": allow_unknown_safety,
        "safety_coverage": SF.coverage(), "gate": gate, "suggest": suggest,
        "unit_factor": unit_factor, "hv_ceiling": hv_ceiling,
        "mortgage_rate": MORTGAGE_30Y_RATE,
        "has_mf_data": has_mf_data,
        "data_pending": not has_mf_data,
        "state_struct": state_struct,
        "has_history": has_history,
        "funnel": funnel, "emptied_by": emptied_by, "offered": offered,
        "score_info": score_info, "national": national, "place": place,
        "climate_meta": climate.get("_meta"), "hazards_meta": hazards.get("_meta"),
        "rent_tiers": RL.TIER_BY_KEY, "rent_mix": rent_mix,
        **finder_ctx,
    })


@app.get("/fair-value")
async def fair_value_redirect(state: str = ""):
    """The inflation-adjusted-payment "fair value" page was retired after its
    audit (DECISIONS 2026-10-01); its question is answered by housing
    affordability against a fixed 2019 baseline."""
    st = (state or "").strip().upper()
    q = f"?state={st}" if len(st) == 2 and st.isalpha() else ""
    return RedirectResponse(url=f"/housing-affordability{q}", status_code=301)


@app.get("/housing-affordability")
def housing_affordability(request: Request, state: str = ""):
    """What it takes for the median household to buy the typical home, today
    against 2019: payment-to-income, the price affordable at 30% of income,
    and how much of the change came from prices, rates, incomes and
    insurance. Every state and the nation are the typical household of the
    same ZIPs at both ends (affordability.py)."""
    import affordability as AF
    from data_providers import CHOROPLETH_STATES, MORTGAGE_30Y_RATE, MORTGAGE_30Y_OBS_DATE
    ctx = AF.page(MORTGAGE_30Y_RATE, _fmt_obs_date(MORTGAGE_30Y_OBS_DATE), state,
                  {k: {"name": v.get("name", k), "fips": v.get("fips")} for k, v in CHOROPLETH_STATES.items()})
    return templates.TemplateResponse("housing_affordability.html", {"request": request, **ctx})

@app.get("/conditions")
def conditions_page(request: Request):
    """Market conditions by state: Realtor.com listings for the latest month
    against the same month a year earlier, one measure at a time
    (conditions.py). Redfin's sale-to-list and months of supply — sales
    measures Realtor.com does not publish — are shown as of their last
    period (May 2026), marked frozen."""
    import conditions as CD
    from data_providers import CHOROPLETH_STATES
    redfin, period = CD.load_redfin()
    ctx = CD.page({k: {"name": v.get("name", k), "fips": v.get("fips")} for k, v in CHOROPLETH_STATES.items()},
                  redfin, period)
    return templates.TemplateResponse("conditions.html", {"request": request, **ctx})


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
    # _safe_redirect, not startswith("/"): "//elsewhere.com" starts with a
    # slash too, and a browser follows it off-site.
    redirect_to = _safe_redirect(request.cookies.get(OAUTH_REDIRECT_COOKIE, "/pipeline"))
    secure = request.url.scheme == "https"
    resp = RedirectResponse(redirect_to, status_code=303)
    resp.set_cookie(SESSION_COOKIE, token, max_age=60 * 60 * 24 * 30,
                    httponly=True, secure=secure, samesite="lax")
    resp.delete_cookie(OAUTH_STATE_COOKIE)
    resp.delete_cookie(OAUTH_REDIRECT_COOKIE)
    return resp


# ── Email sign-in links: a way in that Google cannot block ───────────
# The Google OAuth app is Workspace-internal, so Google turns away any
# account outside the organisation (Error 403: org_internal) — the owner's
# own Gmail among them — before this server is involved. These routes sign
# in an address on ADMIN_EMAILS / SALES_EMAILS by mailing it a one-time link.
EMAIL_LINK_MAX_PER_ADDRESS = 3       # per 15 minutes
EMAIL_LINK_MAX_PER_IP = 10
_EMAIL_LINK_SENT: dict[str, list[float]] = {}


def _email_link_rate_ok(key: str, limit: int, window: float = 900.0, now: float | None = None) -> bool:
    import time as _time
    t = now if now is not None else _time.time()
    hits = [x for x in _EMAIL_LINK_SENT.get(key, []) if t - x < window]
    if len(hits) >= limit:
        _EMAIL_LINK_SENT[key] = hits
        return False
    _EMAIL_LINK_SENT[key] = hits + [t]
    return True


def _site_base(request: Request) -> str:
    """Where the emailed link points. PUBLIC_BASE_URL when set; otherwise the
    Host this request arrived on — never X-Forwarded-Host, which a caller can
    set to have a genuine link mailed out pointing at their own server."""
    base = os.environ.get("PUBLIC_BASE_URL", "").strip().rstrip("/")
    if base:
        return base
    host = request.headers.get("host") or request.url.netloc
    proto = request.headers.get("x-forwarded-proto", request.url.scheme)
    if proto not in ("http", "https"):
        proto = "https"
    return f"{proto}://{host}"


@app.post("/auth/email/start")
async def auth_email_start(request: Request):
    """Mail a one-time sign-in link to an address on the access list. The
    answer is the same whether or not the address is on it."""
    from urllib.parse import quote
    from auth import role_for_email, make_email_link, EMAIL_LINK_TTL
    form = await request.form()
    email = str(form.get("email") or "").strip().lower()[:254]
    redirect = _safe_redirect(str(form.get("redirect") or "/"))
    ip = request.client.host if request.client else "?"
    allowed = (bool(email) and "@" in email
               and _email_link_rate_ok(f"ip:{ip}", EMAIL_LINK_MAX_PER_IP)
               and _email_link_rate_ok(f"to:{email}", EMAIL_LINK_MAX_PER_ADDRESS)
               and bool(role_for_email(email)))
    if allowed:
        token = make_email_link(email)
        if not token:
            logger.error("email sign-in: SESSION_SECRET not set — no link sent")
        else:
            from crm import send_via_resend
            link = f"{_site_base(request)}/auth/email/verify?t={quote(token)}&redirect={quote(redirect)}"
            res = send_via_resend(
                to_email=email, subject="Your Market Pulse sign-in link",
                body=(f"Sign in to Market Pulse:\n\n{link}\n\n"
                      f"The link works once and expires in {EMAIL_LINK_TTL // 60} minutes. "
                      "If you didn't ask for it, ignore this email."))
            if not res.get("ok"):
                logger.error("email sign-in: send failed: %s", res.get("error"))
    return templates.TemplateResponse("email_link.html", {
        "request": request, "state": "sent", "email": email, "redirect": redirect,
        "ttl_min": EMAIL_LINK_TTL // 60})


@app.get("/auth/email/verify")
async def auth_email_verify_page(request: Request, t: str = "", redirect: str = "/"):
    """The link lands here: a confirm button, not the sign-in itself, because
    mail scanners open links on their own and would use it up."""
    from auth import read_email_link
    data = read_email_link(t)
    return templates.TemplateResponse("email_link.html", {
        "request": request, "state": "confirm" if data else "invalid",
        "email": (data or {}).get("email", ""), "token": t,
        "redirect": _safe_redirect(redirect)})


@app.post("/auth/email/verify")
async def auth_email_verify(request: Request):
    """Use the link once: set the same 30-day session Google sign-in sets."""
    from auth import SESSION_COOKIE, consume_email_link, role_for_email, make_session
    form = await request.form()
    email = consume_email_link(str(form.get("t") or ""))
    role = role_for_email(email) if email else None
    token = make_session(email, role) if role else ""
    if not token:
        return templates.TemplateResponse("email_link.html", {
            "request": request, "state": "invalid", "email": "", "redirect": "/"}, status_code=400)
    resp = RedirectResponse(_safe_redirect(str(form.get("redirect") or "/")), status_code=303)
    resp.set_cookie(SESSION_COOKIE, token, max_age=60 * 60 * 24 * 30,
                    httponly=True, secure=(request.url.scheme == "https"
                                           or request.headers.get("x-forwarded-proto") == "https"),
                    samesite="lax")
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



# ─── /zip/{zip} detail page (Phase A.1) ────────────────────────────
# Server-rendered ZIP detail page with multi-horizon forecast,
# historical chart, and county/state comparison strip. Linked from
# the popup's "View full report →" button.

@app.get("/zip/{zip}")
async def zip_page(request: Request, zip: str):
    """One ZIP for an investor or an owner-occupant: what it costs, what it
    rents for, what the market is doing and when, what could go wrong.

    Reads data/zip_profile.db (scripts/build_zip_profile.py). No score and
    no forecast — measurements with their sources and months, and an
    underwriting card whose arithmetic runs in underwrite.py through
    /api/zip/{zip}/underwrite."""
    import zip_page as ZP
    from data_providers import MORTGAGE_30Y_RATE, MORTGAGE_30Y_OBS_DATE
    z = zip.strip()
    if not (len(z) == 5 and z.isdigit()):
        return RedirectResponse(url="/map", status_code=302)
    page = ZP.build_page(z, MORTGAGE_30Y_RATE, MORTGAGE_30Y_OBS_DATE)
    if page is None:
        return templates.TemplateResponse("zip_profile.html", {
            "request": request, "missing": z, "page": None}, status_code=404)
    return templates.TemplateResponse("zip_profile.html", {
        "request": request, "missing": None, "page": page})


@app.get("/api/zip/{zip}/underwrite")
async def api_zip_underwrite(request: Request, zip: str):
    """The underwriting card's arithmetic: this ZIP's defaults, overridden
    by any query parameter (price, rent, units, tax, ins, hoa, util, vac,
    mgmt, maint, capex, rate, down, closing, pairing, o_down, o_rate,
    o_tax, o_ins, o_hoa, o_pmi, o_debt)."""
    import zip_page as ZP
    from data_providers import MORTGAGE_30Y_RATE
    row = ZP.load_row(zip.strip())
    if row is None:
        return JSONResponse({"error": "unknown ZIP"}, status_code=404)
    res = ZP.underwrite_zip(row, dict(request.query_params), MORTGAGE_30Y_RATE)
    return JSONResponse({k: res[k] for k in ("used", "investor", "owner", "chosen")})


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


_HUNDRED_SNAPSHOT_DIR = Path(__file__).resolve().parent / "data" / "hundred_snapshots"

_FCFQ_SNAPSHOT_DIR = Path(__file__).resolve().parent / "data" / "fcf_quality_snapshots"


@app.get("/api/hundred/snapshots")
async def api_hundred_snapshot_list():
    if not _HUNDRED_SNAPSHOT_DIR.exists():
        return JSONResponse({"months": [], "latest": None})
    months = sorted((p.stem for p in _HUNDRED_SNAPSHOT_DIR.glob("*.json")), reverse=True)
    return JSONResponse({"months": months, "latest": months[0] if months else None})


@app.get("/api/hundred/snapshot/{month}")
async def api_hundred_snapshot(month: str):
    if len(month) != 7 or month[4] != "-" or not (month[:4].isdigit() and month[5:].isdigit()):
        return JSONResponse({"error": "month must be YYYY-MM"}, status_code=400)
    path = _HUNDRED_SNAPSHOT_DIR / f"{month}.json"
    if not path.exists():
        return JSONResponse({"error": f"no snapshot for {month}"}, status_code=404)
    try:
        data = json.loads(path.read_text())
        # BACKFILL THE CLUSTERING READOUT for snapshots built before it
        # existed. Computed here rather than in the page so there is one
        # implementation and it is the domain module's — a second copy in
        # JavaScript is how the sector ordering came to disagree with
        # itself. The file on disk stays exactly as the Action wrote it.
        meta = data.get("_meta") or {}
        cen = meta.get("census")
        if isinstance(cen, dict) and not cen.get("clustering"):
            try:
                import checklist as _C
                cen["clustering"] = _C.value_clustering(data.get("companies") or [])
            except Exception:
                logger.exception("hundred: clustering backfill failed")
        # HAND ENTRIES ARE MERGED HERE, NOT BUILT IN. The monthly Action
        # reconstructs every row from filings and replaces this file
        # wholesale, so a reading written into it would not survive the
        # next run. Merging on read also means the Action needs no
        # database and no change at all.
        try:
            import hand as _H
            from database import hundred_hand_all
            entries = hundred_hand_all()
            if entries:
                meta["hand"] = _H.merge(data.get("companies") or [], entries)
            meta["hand_rules"] = _H.RULES
        except Exception:
            logger.exception("hundred: hand-entry merge failed")
        data["_meta"] = meta
        return JSONResponse(data)
    except Exception as e:
        return JSONResponse({"error": f"failed to read snapshot: {e}"}, status_code=500)


@app.post("/api/hundred/hand")
async def api_hundred_hand_save(request: Request):
    """Record one hand-read criterion.

    Body: {ticker, criterion, value?, note?, source, read_on?}

    Admin-gated because it changes what the board shows. Validation is
    hand.validate(), which REFUSES rather than coerces — this is the one
    input on the page that costs a person an hour of reading, and silently
    storing a share count where a percent belongs would waste it in a way
    nobody would ever notice.
    """
    gate = _admin_gate(request)
    if gate:
        return gate
    import hand as H
    from database import hundred_hand_save
    body = await request.json()
    ticker = str(body.get("ticker", "")).strip().upper()
    if not ticker:
        return JSONResponse({"error": "ticker required"}, status_code=400)
    try:
        entry = H.validate(body.get("criterion"), body.get("value"),
                           note=body.get("note", ""),
                           source=body.get("source", ""),
                           read_on=body.get("read_on", ""))
    except H.Invalid as e:
        return JSONResponse({"error": str(e)}, status_code=400)
    who = ""
    try:
        from auth import SESSION_COOKIE, verify_session
        who = (verify_session(request.cookies.get(SESSION_COOKIE, "")) or {}).get("email", "")
    except Exception:
        pass
    ok = hundred_hand_save(ticker, entry, entered_by=who)
    if not ok:
        return JSONResponse(
            {"error": "could not save — no database is configured, and this "
                      "is the only copy, so nothing was written"},
            status_code=503)
    return JSONResponse({"ok": True, "ticker": ticker, "entry": entry})


@app.delete("/api/hundred/hand/{ticker}/{criterion}")
async def api_hundred_hand_delete(ticker: str, criterion: int, request: Request):
    gate = _admin_gate(request)
    if gate:
        return gate
    from database import hundred_hand_delete
    ok = hundred_hand_delete(ticker, criterion)
    return JSONResponse({"ok": ok, "ticker": ticker.upper(), "criterion": criterion},
                        status_code=200 if ok else 503)


@app.get("/api/hundred/hand")
async def api_hundred_hand_list(request: Request):
    """Every hand entry. Readable without a gate: it is the reader's own
    research on their own board, and the write path is what needs guarding."""
    from database import hundred_hand_all
    return JSONResponse({"entries": hundred_hand_all(),
                         "can_edit": _check_admin_token(request)})


_SCHLOSS_SNAPSHOT_DIR = Path(__file__).resolve().parent / "data" / "schloss_snapshots"


@app.get("/api/schloss/snapshots")
async def api_schloss_snapshot_list():
    """Every month the Schloss board has been recorded.

    The board itself is overwritten each run, so these are the only
    point-in-time record — and the only way a forward-return study on this
    screen can ever avoid survivorship bias, because a company captured
    here stays captured after it delists.
    """
    if not _SCHLOSS_SNAPSHOT_DIR.exists():
        return JSONResponse({"months": [], "latest": None})
    months = sorted(p.stem for p in _SCHLOSS_SNAPSHOT_DIR.glob("*.json"))
    return JSONResponse({"months": months,
                         "latest": months[-1] if months else None})


@app.get("/api/schloss/snapshot/{month}")
async def api_schloss_snapshot(month: str):
    if len(month) != 7 or month[4] != "-" or not (month[:4].isdigit() and month[5:].isdigit()):
        return JSONResponse({"error": "month must be YYYY-MM"}, status_code=400)
    path = _SCHLOSS_SNAPSHOT_DIR / f"{month}.json"
    if not path.exists():
        return JSONResponse({"error": f"no snapshot for {month}"}, status_code=404)
    try:
        return JSONResponse(json.loads(path.read_text()))
    except Exception as e:
        return JSONResponse({"error": f"failed to read snapshot: {e}"}, status_code=500)


# ── permit-moat watchlist ────────────────────────────────────────────
#
# The one board here that is not a screener. It holds the user to a
# decision they wrote down while calm, so the routes are shaped around
# refusing things: no endpoint below can talk its way past a rubric
# gate, clear a trigger, or overwrite a locked plan without saying so.
# All the decision logic lives in moats.py and is proved offline; these
# handlers validate, persist, and re-read.
def _moat_now():
    # Imported locally: this module's top-level line is `from datetime
    # import date`, so the bare name `datetime` is NOT bound here and
    # using it would NameError on the first price anyone entered.
    from datetime import datetime as _dt
    return _dt.now()


def _moat_view(request: Request) -> dict:
    """Everything the page needs, assembled once."""
    import moats as M
    from database import moat_all
    rows = moat_all()
    now = _moat_now()
    for r in rows:
        pos = r.get("position") or {}
        r["distance_pct"] = M.distance_pct(pos.get("lastPrice"),
                                           pos.get("targetPrice"))
        r["gauge"] = M.gauge(pos.get("lastPrice"), pos.get("targetPrice"))
        r["staleness"] = M.price_staleness(pos, now)
        r["needs_ack"] = M.needs_acknowledgement(pos)
        r["needs_triage"] = M.needs_triage(r, now)
        r["age"] = M.relative_age(r.get("addedAt"), now)
        r["score_label"] = M.display_score(r.get("rubric"))
        # A holding whose NEWEST rubric fails is flagged, never demoted.
        # A stage that changed itself overnight would be a decision
        # nobody made.
        rb = r.get("rubric") or {}
        r["rubric_failing"] = bool(rb) and rb.get("passed") is False
        # Grandfathering holds a seed at QUALIFIED without a rubric; it is
        # NOT enough to arm one. The page reads these three to route ARM
        # to the right place: the rubric form when the questions have not
        # been answered, the refusal when they have and a gate says no,
        # and the arming form only when neither is true. Sending someone
        # into the arming form and refusing them after they fill it in is
        # the same waste as rejecting a rubric after they finish it.
        r["rubric_complete"] = M.rubric_is_complete_for_arming(rb)
        r["rubric_failures"] = M.gate_failures(rb) if r["rubric_complete"] else []
        r["rubric_armable"] = r["rubric_complete"] and not r["rubric_failures"]
    return {"rows": rows, "counts": M.counts(rows, now)}


def _moat_find(rows: list, holding_id: int) -> dict | None:
    return next((r for r in rows if r.get("id") == holding_id), None)


def _moat_jsonable(value):
    """Dates and datetimes to ISO strings, everywhere, recursively.

    Postgres hands back real date/datetime objects and the default JSON
    encoder refuses them, so /api/moats returned a 500 and the board
    rendered empty even though the page itself was a clean 200. Applied
    only AFTER the derived fields are computed — needs_triage,
    price_staleness and relative_age all do arithmetic on these and
    would break on strings.
    """
    from datetime import date as _d, datetime as _dt
    if isinstance(value, (_d, _dt)):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: _moat_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_moat_jsonable(v) for v in value]
    return value


def _moat_reject(reasons, status: int = 400):
    """A refusal always names what would have to be different."""
    if isinstance(reasons, str):
        reasons = [reasons]
    return JSONResponse({"error": " ".join(reasons), "reasons": list(reasons)},
                        status_code=status)


@app.get("/moats")
async def moats_page(request: Request):
    """Permit moats — a watchlist for assets that cannot be replicated.

    A mineral deposit, an adjudicated water right, an NRC license, a
    Jones Act hull, consecrated cemetery land. The strategy is to wait
    for one of these to fall to a pre-set price and then buy, which only
    works if you can act when the price actually falls — and the price
    only falls when the news is bad. So this page is a commitment
    device first and a list second.

    It recommends nothing. It shows the user their own targets and their
    own words back.
    """
    view = _moat_view(request)
    import moats as M
    return templates.TemplateResponse("moats.html", {
        "request": request,
        "rows": view["rows"],
        "counts": view["counts"],
        "can_edit": _check_admin_token(request),
        "moat_types": M.MOAT_TYPES,
        "sentiments": M.SENTIMENTS,
        "permit_trends": M.PERMIT_TRENDS,
        "terminal_demand": M.TERMINAL_DEMAND,
        "cheap_because": M.CHEAP_BECAUSE,
        "score_criteria": M.SCORE_CRITERIA,
        "armed_cap": M.ARMED_CAP,
        "stale_days": M.STALE_DAYS,
        "decay_days": M.DECAY_DAYS,
        "evidence_min": M.EVIDENCE_MIN,
        "naics_targets": M.NAICS_TARGETS,
        "default_ceiling": M.DEFAULT_CAP_CEILING,
    })


@app.get("/api/moats")
async def api_moats_list(request: Request):
    view = _moat_view(request)
    return JSONResponse(_moat_jsonable(
        {**view, "can_edit": _check_admin_token(request)}), status_code=200)


@app.post("/api/moats/candidate")
async def api_moats_add(request: Request):
    """Ticker, name, why you noticed it. Nothing else, ever.

    This has to survive being done one-handed on a phone in under
    fifteen seconds. Every extra required field is a reason not to write
    the name down at all, and a name not written down is the one certain
    way to lose it.
    """
    gate = _admin_gate(request)
    if gate:
        return gate
    import moats as M
    from database import moat_add
    try:
        cand = M.validate_candidate(await request.json())
    except M.Invalid as e:
        return _moat_reject(str(e))
    hid = moat_add(cand)
    if hid is None:
        return _moat_reject(f"{cand['ticker']} is already on the list. "
                            f"Re-adding would overwrite what is there.", 409)
    return JSONResponse({"ok": True, "id": hid, "ticker": cand["ticker"]})


@app.post("/api/moats/{holding_id}/fields")
async def api_moats_fields(holding_id: int, request: Request):
    """Save the thesis fields. Promotion checks them; this only stores."""
    gate = _admin_gate(request)
    if gate:
        return gate
    import moats as M
    from database import moat_update
    body = await request.json()
    allowed = ("assetLine", "thesis", "invalidation", "moatType",
               "sentiment", "name", "exchange", "sourceNote")
    fields = {k: (str(v).strip() if v is not None else None)
              for k, v in body.items() if k in allowed}
    if fields.get("moatType") and fields["moatType"] not in M.MOAT_TYPES:
        return _moat_reject("Unknown moat type.")
    if fields.get("sentiment") and fields["sentiment"] not in M.SENTIMENTS:
        return _moat_reject("Unknown sentiment.")
    if not fields:
        return _moat_reject("Nothing to save.")
    moat_update(holding_id, fields)
    return JSONResponse({"ok": True})


@app.post("/api/moats/{holding_id}/rubric")
async def api_moats_rubric(holding_id: int, request: Request):
    """Record an assessment as a NEW VERSION, and report the gates.

    Always stored, pass or fail. A rubric that failed is the most useful
    thing in the file two years later, and one that is only kept when it
    agrees with you is not a record.
    """
    gate = _admin_gate(request)
    if gate:
        return gate
    import moats as M
    from database import moat_rubric_add, moat_update
    try:
        rubric = M.validate_rubric(await request.json())
    except M.Invalid as e:
        return _moat_reject(str(e))
    version = moat_rubric_add(holding_id, rubric)
    if version is None:
        return _moat_reject("Could not save the rubric.", 500)
    moat_update(holding_id, {})          # counts as touching it
    ev = M.evaluate(rubric)
    return JSONResponse({"ok": True, "version": version, "score": ev["score"],
                         "passed": ev["passed"], "failures": ev["failures"]})


@app.get("/api/moats/{holding_id}/rubrics")
async def api_moats_rubric_history(holding_id: int, request: Request):
    """Every version, newest first. Nothing here is ever overwritten."""
    from database import moat_rubric_history
    return JSONResponse(_moat_jsonable(
        {"versions": moat_rubric_history(holding_id)}))


@app.post("/api/moats/{holding_id}/promote")
async def api_moats_promote(holding_id: int, request: Request):
    """CANDIDATE -> QUALIFIED. Blocked by either gate, with NO override.

    The refusal names which gate failed and what it means, because a
    refusal that does not say what would have to be different is a wall
    rather than a gate.
    """
    gate = _admin_gate(request)
    if gate:
        return gate
    import moats as M
    from database import moat_update
    view = _moat_view(request)
    h = _moat_find(view["rows"], holding_id)
    if not h:
        return _moat_reject("No such holding.", 404)
    verdict = M.can_promote(h, h.get("rubric"))
    if not verdict["ok"]:
        return JSONResponse({"error": " ".join(verdict["reasons"]),
                             "reasons": verdict["reasons"],
                             "failures": verdict["failures"]}, status_code=400)
    moat_update(holding_id, {"stage": "QUALIFIED"})
    return JSONResponse({"ok": True, "stage": "QUALIFIED"})


@app.post("/api/moats/{holding_id}/arm")
async def api_moats_arm(holding_id: int, request: Request):
    """QUALIFIED -> ARMED. Needs a target, a price, and a locked plan.

    The armed cap is enforced HERE, against a freshly counted board,
    rather than trusting a number the client sent.
    """
    gate = _admin_gate(request)
    if gate:
        return gate
    import moats as M
    from database import moat_update, moat_position_upsert
    body = await request.json()
    view = _moat_view(request)
    h = _moat_find(view["rows"], holding_id)
    if not h:
        return _moat_reject("No such holding.", 404)

    pos = dict(h.get("position") or {})
    for key in ("targetPrice", "lastPrice"):
        if body.get(key) not in (None, ""):
            try:
                v = float(body[key])
            except (TypeError, ValueError):
                return _moat_reject(f"{key} must be a number.")
            if v <= 0:
                return _moat_reject("Prices have to be greater than zero.")
            pos[key] = v
    if (body.get("plan") or "").strip():
        pos["plan"] = body["plan"].strip()

    verdict = M.can_arm(h, pos, view["counts"]["armed"], h.get("rubric"))
    if not verdict["ok"]:
        # `needs_rubric` lets the client route into the rubric form
        # instead of showing a dead button.
        return JSONResponse({"error": " ".join(verdict["reasons"]),
                             "reasons": verdict["reasons"],
                             "at_cap": verdict["at_cap"],
                             "needs_rubric": verdict["needs_rubric"],
                             "armed": view["counts"]["armed"]}, status_code=400)

    now = _moat_now()
    write = {"targetPrice": pos["targetPrice"], "lastPrice": pos["lastPrice"],
             "lastPriceAt": now}
    if not (h.get("position") or {}).get("planLockedAt"):
        write.update(M.lock_plan({}, pos["plan"], now))
    # Arming can itself trigger, when the price is already at the target.
    res = M.apply_price({**pos, **write}, pos["lastPrice"], now)
    write.update({k: v for k, v in res["changed"].items() if k != "lastPriceAt"})
    moat_position_upsert(holding_id, write)
    moat_update(holding_id, {"stage": "ARMED"})
    return JSONResponse({"ok": True, "stage": "ARMED",
                         "triggered": res["triggered"]})


@app.post("/api/moats/{holding_id}/disarm")
async def api_moats_disarm(holding_id: int, request: Request):
    """ARMED -> QUALIFIED. The target, plan and notes all stay put.

    Disarming frees a slot; it does not erase the thinking. Re-arming
    later should find the plan exactly where it was left.
    """
    gate = _admin_gate(request)
    if gate:
        return gate
    from database import moat_update
    moat_update(holding_id, {"stage": "QUALIFIED"})
    return JSONResponse({"ok": True, "stage": "QUALIFIED"})


@app.post("/api/moats/{holding_id}/target")
async def api_moats_target(holding_id: int, request: Request):
    """Change the target on a holding that is already armed.

    THIS EXISTS BECAUSE THE TARGET INPUT WAS INERT. The page posted a
    target edit to /arm, which refuses anything not in QUALIFIED — so on
    every armed row, the field the whole page is built around silently
    failed with "only a qualified holding can be armed". The value the
    user typed stayed on screen while the stored target never moved.

    A target change re-runs the SAME trigger rule as a price change,
    against the price already on file: moving a target up to meet the
    market is exactly as much a crossing as the price falling to meet
    the target, and the plan is owed either way.
    """
    gate = _admin_gate(request)
    if gate:
        return gate
    import moats as M
    from database import moat_position_upsert
    body = await request.json()
    view = _moat_view(request)
    h = _moat_find(view["rows"], holding_id)
    if not h:
        return _moat_reject("No such holding.", 404)
    if h.get("stage") not in ("ARMED", "QUALIFIED"):
        return _moat_reject("Only an armed or qualified holding has a target.")
    try:
        target = float(body.get("target"))
    except (TypeError, ValueError):
        return _moat_reject("The target must be a number.")
    if target <= 0:
        return _moat_reject("A target has to be greater than zero.")

    pos = dict(h.get("position") or {})
    write = {"targetPrice": target}
    res = None
    if pos.get("lastPrice"):
        res = M.apply_price({**pos, "targetPrice": target}, pos["lastPrice"],
                            _moat_now())
        # Keep the price stamp — the price itself did not change, only the
        # target did, and re-dating it would hide a genuinely stale quote.
        write.update({k: v for k, v in res["changed"].items()
                      if k != "lastPriceAt"})
    moat_position_upsert(holding_id, write)
    return JSONResponse(_moat_jsonable({
        "ok": True, "targetPrice": target,
        "newly_triggered": bool(res and res["newly_triggered"]),
        "distance_pct": res["distance_pct"] if res else None,
        "gauge": res["gauge"] if res else M.gauge(None, target),
    }))


@app.post("/api/moats/{holding_id}/price")
async def api_moats_price(holding_id: int, request: Request):
    """Record a price, and report whether it just crossed the target."""
    gate = _admin_gate(request)
    if gate:
        return gate
    import moats as M
    from database import moat_position_upsert
    body = await request.json()
    view = _moat_view(request)
    h = _moat_find(view["rows"], holding_id)
    if not h:
        return _moat_reject("No such holding.", 404)
    try:
        res = M.apply_price(h.get("position") or {}, body.get("price"), _moat_now())
    except M.Invalid as e:
        return _moat_reject(str(e))
    moat_position_upsert(holding_id, res["changed"])
    now = _moat_now()
    return JSONResponse({
        "ok": True,
        "newly_triggered": res["newly_triggered"],
        "triggered": res["triggered"],
        "distance_pct": res["distance_pct"],
        "gauge": res["gauge"],
        "staleness": M.price_staleness({**(h.get("position") or {}),
                                        **res["changed"]}, now),
        "plan": (h.get("position") or {}).get("plan") or "",
        "plan_locked_at": str((h.get("position") or {}).get("planLockedAt") or ""),
    })


@app.post("/api/moats/{holding_id}/acknowledge")
async def api_moats_acknowledge(holding_id: int, request: Request):
    """The user confirms they have read their own plan.

    This is the whole point of the app. It is a separate, explicit
    action precisely so it cannot happen by scrolling past.
    """
    gate = _admin_gate(request)
    if gate:
        return gate
    from database import moat_position_upsert
    moat_position_upsert(holding_id, {"acknowledged": True})
    return JSONResponse({"ok": True})


@app.post("/api/moats/{holding_id}/plan")
async def api_moats_plan(holding_id: int, request: Request):
    """Save or change the plan. Changing a locked one needs confirmation.

    The previous text is appended to the notes with the date it was
    committed to. Rewriting the thesis mid-drawdown is the failure this
    app exists to defend against, so the friction is deliberate and the
    old words are kept rather than replaced.
    """
    gate = _admin_gate(request)
    if gate:
        return gate
    import moats as M
    from database import moat_position_upsert
    body = await request.json()
    view = _moat_view(request)
    h = _moat_find(view["rows"], holding_id)
    if not h:
        return _moat_reject("No such holding.", 404)
    pos = h.get("position") or {}
    try:
        changed = M.edit_plan(pos, body.get("plan"), _moat_now(),
                              confirmed=bool(body.get("confirmed")))
    except M.Invalid as e:
        return JSONResponse({"error": str(e),
                             "needs_confirmation": bool(pos.get("planLockedAt")),
                             "locked_at": str(pos.get("planLockedAt") or ""),
                             "current_plan": pos.get("plan") or ""},
                            status_code=400)
    if changed:
        moat_position_upsert(holding_id, changed)
    return JSONResponse({"ok": True, "changed": bool(changed)})


@app.post("/api/moats/{holding_id}/note")
async def api_moats_note(holding_id: int, request: Request):
    """Append-only. Existing entries are never rewritten."""
    gate = _admin_gate(request)
    if gate:
        return gate
    import moats as M
    from database import moat_position_upsert, moat_update
    body = await request.json()
    view = _moat_view(request)
    h = _moat_find(view["rows"], holding_id)
    if not h:
        return _moat_reject("No such holding.", 404)
    try:
        changed = M.append_note(h.get("position") or {}, body.get("note"),
                                _moat_now())
    except M.Invalid as e:
        return _moat_reject(str(e))
    moat_position_upsert(holding_id, changed)
    moat_update(holding_id, {})
    return JSONResponse({"ok": True})


@app.post("/api/moats/{holding_id}/archive")
async def api_moats_archive(holding_id: int, request: Request):
    """Always requires a reason. Never deletes.

    In a year this is the only thing that will explain the decision, and
    the accumulated record of what was passed on is the most valuable
    thing this app builds over a decade.
    """
    gate = _admin_gate(request)
    if gate:
        return gate
    import moats as M
    from database import moat_update
    body = await request.json()
    verdict = M.can_archive(body.get("reason"))
    if not verdict["ok"]:
        return _moat_reject(verdict["reasons"])
    moat_update(holding_id, {"stage": "ARCHIVED",
                             "archiveReason": verdict["reason"],
                             "archivedAt": _moat_now()})
    return JSONResponse({"ok": True, "stage": "ARCHIVED"})


@app.post("/api/moats/{holding_id}/restore")
async def api_moats_restore(holding_id: int, request: Request):
    """Back to CANDIDATE, with the archive reason kept as history."""
    gate = _admin_gate(request)
    if gate:
        return gate
    from database import moat_update
    moat_update(holding_id, {"stage": "CANDIDATE", "archivedAt": None})
    return JSONResponse({"ok": True, "stage": "CANDIDATE"})


@app.post("/api/moats/import/preview")
async def api_moats_import_preview(request: Request):
    """Parse and filter a CSV. WRITES NOTHING.

    Reports every rejected row in a named bucket, and the buckets sum to
    the input. "We found 140 matches" means nothing without "out of
    what, and why not the rest" — and a filter that reports only its
    winners cannot be checked.
    """
    gate = _admin_gate(request)
    if gate:
        return gate
    import moats as M
    from database import moat_all
    body = await request.json()
    try:
        parsed = M.parse_csv(body.get("csv"))
    except M.Invalid as e:
        return _moat_reject(str(e))
    ceiling = body.get("ceiling", M.DEFAULT_CAP_CEILING)
    if ceiling in ("", None):
        ceiling = None
    else:
        try:
            ceiling = float(ceiling)
        except (TypeError, ValueError):
            return _moat_reject("The market cap ceiling must be a number, "
                                "or blank for no ceiling.")
    existing = [h["ticker"] for h in moat_all()]
    result = M.filter_import(parsed["rows"], existing, cap_ceiling=ceiling)
    return JSONResponse({**result, "skipped_rows": parsed["skipped"],
                         "columns": parsed["columns"], "ceiling": ceiling})


@app.post("/api/moats/import/commit")
async def api_moats_import_commit(request: Request):
    """Bulk-add the matched rows as candidates.

    Re-filtered server-side against a freshly read watchlist rather than
    trusting the preview the client is holding — the two are separated
    by however long the user spent reading it.
    """
    gate = _admin_gate(request)
    if gate:
        return gate
    import moats as M
    from database import moat_all, moat_add
    body = await request.json()
    rows = body.get("rows") or []
    if not isinstance(rows, list) or not rows:
        return _moat_reject("Nothing selected to import.")
    existing = [h["ticker"] for h in moat_all()]
    result = M.filter_import(
        [{"ticker": str(r.get("ticker", "")).strip().upper(),
          "name": r.get("name") or "", "naics": M.normalize_naics(r.get("naics")),
          "marketCap": M.parse_market_cap(r.get("marketCap"))} for r in rows],
        existing, cap_ceiling=None)

    added, skipped = [], []
    for r in result["matched"]:
        hid = moat_add({"ticker": r["ticker"], "name": r["name"],
                        "exchange": "", "stage": "CANDIDATE",
                        "sourceNote": r.get("sourceNote") or "",
                        "naics": r["naics"], "marketCapAtAdd": r.get("marketCap")})
        (added if hid else skipped).append(r["ticker"])
    return JSONResponse({"ok": True, "added": added, "skipped": skipped,
                         "already_tracked": [r["ticker"] for r
                                             in result["already_tracked"]],
                         "rejected": result["counts"]["wrong_naics"]})


@app.get("/api/fcf-quality/snapshots")
async def api_fcfq_snapshot_list():
    if not _FCFQ_SNAPSHOT_DIR.exists():
        return JSONResponse({"months": [], "latest": None})
    months = sorted(
        (p.stem for p in _FCFQ_SNAPSHOT_DIR.glob("*.json")),
        reverse=True,
    )
    return JSONResponse({"months": months, "latest": months[0] if months else None})


@app.get("/api/fcf-quality/snapshot/{month}")
async def api_fcfq_snapshot(month: str):
    if len(month) != 7 or month[4] != "-" or not (month[:4].isdigit() and month[5:].isdigit()):
        return JSONResponse({"error": "month must be YYYY-MM"}, status_code=400)
    path = _FCFQ_SNAPSHOT_DIR / f"{month}.json"
    if not path.exists():
        return JSONResponse({"error": f"no snapshot for {month}"}, status_code=404)
    try:
        return JSONResponse(json.loads(path.read_text()))
    except Exception as e:
        return JSONResponse({"error": f"failed to read snapshot: {e}"}, status_code=500)


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

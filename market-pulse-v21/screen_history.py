"""Watch the strict screen change over time. /norcal

The market half of the screen already moves on its own: Zillow/Redfin
refresh zips.db on the 1st of each month, FRED rates weekly, and
norcal.screen() reads all of it live. What was missing is the ability to
SEE it move — a town crossing into budget, or dropping out because its
price ran away, is the single most actionable event this page produces.

This module snapshots each month's result and diffs consecutive months:

  ENTERED   passed everything and is newly inside budget
  LEFT      was buyable, no longer is (with the reason: price, or a gate)
  CHEAPER / PRICIER   still buyable, entry price moved

Snapshots are small JSON (one row per qualifying ZIP), committed by the
monthly Action so history accrues in the repo and survives redeploys.
"""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path

_HIST = Path(__file__).resolve().parent / "data" / "screen_history"

# Price moves smaller than this are noise from the ZHVI revision cycle,
# not news — don't report them as changes.
MATERIAL_PCT = 1.5


def _path(market: str, period: str) -> Path:
    return _HIST / f"{market.lower()}_{period}.json"


def current_period(today: date | None = None) -> str:
    d = today or date.today()
    return f"{d.year:04d}-{d.month:02d}"


def snapshot(res: dict, market: str, period: str | None = None,
             params: dict | None = None) -> dict:
    """Persist one month's screen result. Stores only what a diff needs."""
    period = period or current_period()
    _HIST.mkdir(parents=True, exist_ok=True)
    def rows(lst):
        return {s["zip"]: {"name": s["name"], "region": s["region"],
                           "entry": s["entry_price"], "kind": s["entry_kind"],
                           "minutes": s["minutes"], "anchor": s["anchor"],
                           "safety": (s.get("safety") or {}).get("tier"),
                           "violent": (s.get("safety") or {}).get("violent")}
                for s in lst}
    snap = {"period": period, "market": market,
            "params": params or {},
            "max_purchase": res["power"]["max_purchase"],
            "universe_n": res["universe_n"],
            "buyable": rows(res["buyable"]),
            "aspirational": rows(res["aspirational"])}
    _path(market, period).write_text(json.dumps(snap, indent=1))
    return snap


def load(market: str, period: str) -> dict | None:
    p = _path(market, period)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text())
    except (OSError, ValueError):
        return None


def periods(market: str) -> list[str]:
    if not _HIST.exists():
        return []
    pre = f"{market.lower()}_"
    return sorted(p.stem[len(pre):] for p in _HIST.glob(f"{pre}*.json"))


def diff(market: str, newer: str | None = None,
         older: str | None = None) -> dict | None:
    """Change report between two snapshots (defaults: latest two)."""
    ps = periods(market)
    if len(ps) < 2 and not (newer and older):
        return None
    newer = newer or ps[-1]
    older = older or ps[-2]
    a, b = load(market, older), load(market, newer)
    if not a or not b:
        return None
    ab, bb = a.get("buyable", {}), b.get("buyable", {})
    a_asp, b_asp = a.get("aspirational", {}), b.get("aspirational", {})

    entered, left, moved = [], [], []
    for z, row in bb.items():
        if z not in ab:
            # Why is it new? Either it was above budget, or it wasn't
            # qualifying at all last month.
            was = "was above budget" if z in a_asp else "newly qualifying"
            entered.append({**row, "zip": z, "why": was,
                            "prev_entry": (a_asp.get(z) or {}).get("entry")})
    for z, row in ab.items():
        if z not in bb:
            why = "priced out of budget" if z in b_asp else "no longer passes a gate"
            left.append({**row, "zip": z, "why": why,
                         "new_entry": (b_asp.get(z) or {}).get("entry")})
    for z, row in bb.items():
        if z in ab and ab[z].get("entry") and row.get("entry"):
            prev, now = ab[z]["entry"], row["entry"]
            pct = (now / prev - 1) * 100 if prev else 0.0
            if abs(pct) >= MATERIAL_PCT:
                moved.append({**row, "zip": z, "prev_entry": prev,
                              "pct": round(pct, 1)})
    moved.sort(key=lambda r: r["pct"])
    return {"market": market, "newer": newer, "older": older,
            "entered": entered, "left": left,
            "cheaper": [m for m in moved if m["pct"] < 0],
            "pricier": [m for m in moved if m["pct"] > 0],
            "n_now": len(bb), "n_prev": len(ab),
            "max_purchase_now": b.get("max_purchase"),
            "max_purchase_prev": a.get("max_purchase")}


# ── Freshness of the researched (non-auto-refreshing) layers ─────────
# The market half of the screen refreshes itself; these layers are
# point-in-time research and must age visibly rather than silently.
LAYER_TTL_DAYS = {
    "crime": 400,        # FBI releases annually (Sept/Oct)
    "climate": 3650,     # NOAA normals move on a 10-year cycle
    "hazard": 730,       # FEMA maps + insurance markets shift slowly
    "financing": 120,    # lender terms drift fast
    "proptax": 400,
    "insurance": 400,
    "utilities": 400,
    "inctax": 400,
    "calibration": 400,
}

_LAYER_FILES = {
    "crime": "headroom/crime.json", "calibration": "headroom/calibration.json",
    "financing": "headroom/financing.json", "proptax": "headroom/proptax.json",
    "insurance": "headroom/insurance.json", "utilities": "headroom/utilities.json",
    "inctax": "headroom/inctax.json",
    "climate": "regions_ne_climate.json", "hazard": "regions_ne_hazard.json",
}


def layer_freshness(today: date | None = None) -> list[dict]:
    """Per-layer age + staleness, so the UI can say which numbers are
    live and which are a research snapshot that needs re-running."""
    base = Path(__file__).resolve().parent / "data"
    d = today or date.today()
    out = []
    for name, rel in _LAYER_FILES.items():
        p = base / rel
        if not p.exists():
            out.append({"layer": name, "present": False, "stale": True,
                        "as_of": None, "age_days": None})
            continue
        as_of = None
        try:
            meta = json.loads(p.read_text()).get("_meta", {})
            as_of = meta.get("as_of")
        except (OSError, ValueError):
            meta = {}
        age = None
        if as_of:
            try:
                y, m = (as_of.split("-") + ["01"])[:2]
                age = (d - date(int(y), int(m), 1)).days
            except ValueError:
                age = None
        ttl = LAYER_TTL_DAYS.get(name, 400)
        out.append({"layer": name, "present": True, "as_of": as_of,
                    "age_days": age, "ttl_days": ttl,
                    "stale": bool(age is not None and age > ttl),
                    "status": (meta.get("status") if isinstance(meta, dict) else None)})
    return out

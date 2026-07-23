"""Contractor scope-of-work plan for the SFR remodel budgeter. /value-add

Turns a remodel_budget() dict into a phased construction plan — what gets
done and in what order — and renders it as a letter-size PDF the owner can
hand to contractors for bidding. Phasing follows standard residential
remodel sequencing with the inspection hold points called out (rough
sign-offs before insulation, insulation before drywall). GC overhead &
profit and contingency are carried as project-wide management lines, not
construction phases.

The PDF deliberately contains NO deal economics — no asking price, ARV,
margin, or max offer. Owners send this to the people they negotiate with.
A `dollars=False` variant also strips the budget allowances for clean
competitive bids (scope + sequence only).
"""
from __future__ import annotations

import html
import io
import re
from datetime import date

# Canonical build order. Every REMODEL_ITEMS key and every soft/addon line
# from remodel_budget() must appear in exactly one phase (or MGMT_KEYS) —
# the test suite enforces the mapping stays complete as items are added.
# Keys within a phase are the intra-phase work order.
PLAN_PHASES = [
    {"key": "precon", "title": "Pre-construction, design & permits",
     "desc": "Finalize plans and engineering and pull all permits before any site "
             "work begins. Order long-lead items (windows, cabinets, appliances) at "
             "contract signing to protect the schedule.",
     "keys": ("design", "change_of_use", "permit", "sewer_capacity", "phase1_esa")},
    {"key": "demo", "title": "Hazmat testing, abatement & demolition",
     "desc": "Test for lead paint and asbestos before disturbing any surface; complete "
             "abatement with certified crews first. Then demolish to the agreed scope, "
             "protect what stays, and haul off debris.",
     "keys": ("abatement", "demolition")},
    {"key": "structural", "title": "Foundation, structural & seismic",
     "desc": "All structural corrections while the building is open: foundation work, "
             "seismic strengthening, rot/termite and framing repairs. Engineer sign-off "
             "before anything is covered.",
     "keys": ("foundation_replacement", "foundation_repair", "urm_masonry_retrofit",
              "seismic_retrofit", "termite_dry_rot", "framing_repair")},
    {"key": "shell", "title": "Roof & exterior shell (dry-in)",
     "desc": "Get the building dried in: roof first, then close new or altered openings, "
             "set windows and exterior doors, and side. On conversions the storefront / "
             "roll-up infill is framed and weathered-in before window install. Coordinate "
             "with the rough-in trades: stub roof penetrations (vent stacks, flues) and "
             "set exterior service equipment before final roofing and siding, or carry "
             "the roofer's and sider's return trips for flashing.",
     "keys": ("roof_replace", "storefront_infill", "windows_egress", "windows",
              "exterior_doors", "siding")},
    {"key": "rough", "title": "Rough-in — plumbing, HVAC, electrical",
     "desc": "All in-wall and under-slab systems while walls are open: sewer and "
             "underground drainage first, then HVAC and ducting (the big runs claim "
             "joist bays and chases first), then supply repipe and fire sprinklers, "
             "with the rewire and panel last — wire routes around everything else. "
             "HOLD POINT: rough inspections must be signed off before anything "
             "closes up.",
     "keys": ("sewer_lateral", "dwv_underslab", "hvac", "plumbing_repipe",
              "fire_sprinklers", "electrical_rewire", "electrical_panel")},
    {"key": "drywall", "title": "Insulation & drywall",
     "desc": "Insulate walls and attic after rough sign-off (insulation inspection where "
             "required), then hang, tape and finish drywall.",
     "keys": ("insulation", "drywall")},
    {"key": "finishes", "title": "Interior finishes",
     "desc": "Finish work in this order: kitchen cabinets and counters, bathroom tile "
             "and fixtures, interior doors and trim, then paint, then flooring.",
     "keys": ("kitchen", "bathrooms", "doors_trim", "interior_paint", "flooring")},
    {"key": "punch", "title": "Exterior finish, fixture trim-out & punch list",
     "desc": "Exterior prep and paint, gutters, water heater set, decorative lighting "
             "and finish fixtures. Final inspections, punch-list walk-through, and "
             "handover of warranties and permit cards.",
     "keys": ("exterior_paint", "gutters", "water_heater", "lighting")},
]

# Project-wide lines that are not a construction phase.
MGMT_KEYS = ("gc_op", "contingency")


def build_plan(budget: dict) -> dict:
    """remodel_budget() dict → ordered construction phases. Phases with no
    active items collapse away and the rest renumber 1..n (a cosmetic job
    reads Phase 1-2-3, not Phase 1-7-8). Any line whose key isn't mapped
    lands in a trailing 'Additional scope items' phase so nothing is ever
    silently dropped from the contractor document."""
    rows = {r["key"]: r for r in
            budget["hard_lines"] + budget["soft_lines"] + budget["addons"]}
    used: set[str] = set()
    phases = []
    for ph in PLAN_PHASES:
        items = [rows[k] for k in ph["keys"] if k in rows]
        used.update(k for k in ph["keys"] if k in rows)
        if items:
            phases.append({"n": len(phases) + 1, "title": ph["title"], "desc": ph["desc"],
                           "items": items,
                           "subtotal": sum(r["amount"] for r in items)})
    leftovers = [r for k, r in rows.items() if k not in used and k not in MGMT_KEYS]
    if leftovers:
        phases.append({"n": len(phases) + 1, "title": "Additional scope items",
                       "desc": "Items not yet assigned to a phase — sequence with the GC.",
                       "items": leftovers,
                       "subtotal": sum(r["amount"] for r in leftovers)})
    management = [rows[k] for k in MGMT_KEYS if k in rows]
    return {"phases": phases,
            "phases_subtotal": sum(p["subtotal"] for p in phases),
            "management": management,
            "mgmt_subtotal": sum(r["amount"] for r in management),
            "total": budget["total"]}


def _qty_str(r: dict) -> str:
    b = r.get("basis", "fixed")
    if b in ("sqft", "roof", "ext"):
        return f"{r['qty']:,.0f} sf"
    if b == "bath":
        return f"{int(r['qty'])} bath"
    if b == "window":
        return f"{int(r['qty'])} win"
    return "—"


_PDF_CSS = """
body { font-family: sans-serif; font-size: 9.5pt; color: #1d1d1f; }
h1 { font-size: 16pt; margin: 0 0 2pt 0; }
h2 { font-size: 11.5pt; margin: 14pt 0 2pt 0; color: #16324f; }
p { margin: 3pt 0; }
p.meta { color: #555; margin: 1pt 0; }
p.desc { color: #333; margin: 2pt 0 4pt 0; }
p.foot { color: #777; font-size: 8pt; margin-top: 16pt; }
table { width: 100%; border-collapse: collapse; margin-top: 2pt; }
th { text-align: left; font-size: 8.5pt; color: #666; border-bottom: 1px solid #999;
     padding: 2pt; }
td { border-bottom: 0.5px solid #ddd; padding: 3pt 2pt; }
td.num, th.num { text-align: right; }
tr.sub td { font-weight: bold; border-bottom: none; }
tr.grand td { font-weight: bold; font-size: 10.5pt; border-top: 1.5px solid #444;
              border-bottom: none; }
ul { margin: 3pt 0 0 0; }
li { margin: 2pt 0; }
"""


def _row(r: dict, dollars: bool) -> str:
    label = r["label"]
    if not dollars:
        # Some labels embed a $ rate (e.g. "abatement (pre-1978, $12.0/sqft)")
        # which, with the sqft in the header, reconstructs the allowance —
        # strip it so the competitive-bid variant truly carries no figures.
        label = re.sub(r",?\s*\$[^),]*", "", label)
    cells = f"<td>{html.escape(label)}</td><td>{html.escape(_qty_str(r))}</td>"
    if dollars:
        cells += f"<td class='num'>${r['amount']:,.0f}</td>"
    return f"<tr>{cells}</tr>"


def plan_html(budget: dict, *, address: str = "", dollars: bool = True,
              generated: date | None = None) -> str:
    """The plan document as Story-ready HTML (also handy for tests)."""
    plan = build_plan(budget)
    d = generated or date.today()
    level_label = {"low": "budget", "mid": "mid", "high": "high"}.get(budget["level"], budget["level"])
    spec_bits = [f"{budget['sqft']:,} sqft",
                 f"{budget['beds']} bd / {budget['baths']:g} ba"]
    if budget.get("year_built"):
        spec_bits.append(f"built {budget['year_built']}")
    spec_bits += [f"{budget['scope']} scope", f"{level_label} finish",
                  f"{budget['windows']} windows", f"{budget['state_name']} pricing"]
    if budget.get("conversion"):
        spec_bits.append("commercial → residential conversion")

    head_cols = "<th>Scope item</th><th>Qty</th>" + ("<th class='num'>Allowance</th>" if dollars else "")
    parts = [
        "<h1>Renovation Scope of Work &amp; Phasing Plan</h1>",
        f"<p class='meta'><b>Property:</b> {html.escape(address) if address.strip() else '&mdash;'}</p>",
        f"<p class='meta'>{html.escape(' · '.join(spec_bits))}</p>",
        f"<p class='meta'>Prepared {d.strftime('%B %d, %Y')}</p>",
    ]
    if dollars:
        parts.append(
            "<p class='desc'>Prepared for contractor bidding. Work is to proceed in the "
            "phase order below. Dollar figures are the owner's planning <b>allowances</b> "
            "(labor + materials + disposal per line) — not a bid; contractor to verify "
            "all quantities and site conditions and price accordingly.</p>")
    else:
        parts.append(
            "<p class='desc'>Prepared for contractor bidding — scope and sequence only. "
            "Please price each phase and line item; contractor to verify all quantities "
            "and site conditions.</p>")

    for p in plan["phases"]:
        parts.append(f"<h2>Phase {p['n']} &mdash; {html.escape(p['title'])}</h2>")
        parts.append(f"<p class='desc'>{html.escape(p['desc'])}</p>")
        body = "".join(_row(r, dollars) for r in p["items"])
        sub = (f"<tr class='sub'><td>Phase {p['n']} subtotal</td><td></td>"
               f"<td class='num'>${p['subtotal']:,.0f}</td></tr>") if dollars else ""
        parts.append(f"<table><tr>{head_cols}</tr>{body}{sub}</table>")

    if dollars:
        parts.append("<h2>Project management &amp; reserves</h2>")
        parts.append("<p class='desc'>Carried project-wide, not tied to one phase.</p>")
        mrows = "".join(
            f"<tr><td>{html.escape(r['label'])}</td><td></td><td class='num'>${r['amount']:,.0f}</td></tr>"
            for r in plan["management"])
        parts.append(
            "<table><tr><th>Item</th><th></th><th class='num'>Allowance</th></tr>"
            f"{mrows}"
            f"<tr class='grand'><td>Total project budget</td><td></td>"
            f"<td class='num'>${plan['total']:,.0f}</td></tr></table>")
        band = budget.get("band") or {}
        if band:
            parts.append(
                f"<p class='meta'>Finish-level band: ${band['low']:,.0f} (budget finishes) "
                f"&ndash; ${band['high']:,.0f} (high finishes); this plan is priced at the "
                f"{level_label} level.</p>")

    notes = [
        "Permits must be issued before any construction begins. Schedule every required "
        "inspection: rough plumbing / electrical / mechanical before insulation, "
        "insulation before drywall, and finals at completion.",
        "Long-lead items (windows, cabinetry, appliances) are ordered at contract "
        "signing.",
        "Contractor to be licensed, bonded and insured, and to provide a schedule of "
        "values with progress payments tied to phase completion and lien releases.",
    ]
    if budget.get("pre1978"):
        notes.append("Pre-1978 home: EPA RRP lead-safe work practices apply to every "
                     "trade that disturbs painted surfaces.")
    notes += budget.get("flags", [])
    parts.append("<h2>Notes &amp; conditions</h2>")
    parts.append("<ul>" + "".join(f"<li>{html.escape(n)}</li>" for n in notes) + "</ul>")
    parts.append("<p class='foot'>Prepared with Market Pulse &mdash; remodel budget "
                 "builder. First-pass planning document, not a construction contract "
                 "or bid.</p>")
    return "".join(parts)


def plan_pdf(budget: dict, *, address: str = "", dollars: bool = True,
             generated: date | None = None) -> bytes:
    """Render the phased plan as letter-size PDF bytes (PyMuPDF Story)."""
    import fitz  # heavyweight; imported at call time
    story = fitz.Story(html=plan_html(budget, address=address, dollars=dollars,
                                      generated=generated),
                       user_css=_PDF_CSS)
    buf = io.BytesIO()
    writer = fitz.DocumentWriter(buf)
    mediabox = fitz.paper_rect("letter")
    where = mediabox + (40, 40, -40, -50)
    more = 1
    while more:
        dev = writer.begin_page(mediabox)
        more, _ = story.place(where)
        story.draw(dev)
        writer.end_page()
    writer.close()
    return buf.getvalue()

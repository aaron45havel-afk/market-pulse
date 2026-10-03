"""Broker position exports → /capital holdings, kept in sync.

The owner downloads a positions CSV from the broker (Schwab's "Export" on the
Positions page, one account or all of them; Fidelity's and Vanguard's
downloads are read by their column names) and the page writes each account's
positions into the holdings as one block:

    # sync schwab-456 | Schwab · Individual …456 | as of 2026-09-30 16:05 ET
    VTI, taxable, 52000, 31000
    ...
    # end sync schwab-456

A later export of the same account replaces its block; lines the owner typed
(a 401(k), checking, a fund at another broker) are left alone — the export
fills in what it knows, the owner fills in the rest. Lines the owner typed for
a holding the export now covers are offered for removal so nothing counts
twice.

Rules (DECISIONS.md, 2026-10-03 positions import):
- A position's value is the broker's market value; its cost basis is kept for
  a taxable account only (the only place the switch test taxes a sale).
- Uninvested cash in the account (Schwab's "Cash & Cash Investments") is the
  sweep, written at 0% — the owner can type what it pays. A money-market fund
  is written at the board's T-bill rate, which money funds track. Negative
  cash is a margin loan: it becomes a debt at the rate the owner gives.
- Options, bonds and CDs are not holdings the board can measure: they are
  named with their value and left out, and the check against the broker's
  total says so.
- Only the last digits of an account number are kept, as the broker shows them.
"""
from __future__ import annotations

import csv
import io
import re

import allocation as A
import capital as K

MAX_BYTES = 1_000_000
MAX_POSITIONS = 150                    # holdings hold at most A.MAX_LINES lines in all
MATCH_TOLERANCE = 1.0                  # dollars: the broker's total vs the positions added up

SYNC_START = re.compile(r"^#\s*sync\s+([a-z0-9][a-z0-9\-]{0,40})\s*\|\s*(.*)$", re.I)
SYNC_END = re.compile(r"^#\s*end sync\s+([a-z0-9][a-z0-9\-]{0,40})\s*$", re.I)

# Header names, normalised (lower case, "(...)" dropped), in the order tried.
COLUMNS = {
    "symbol": ("symbol", "ticker"),
    "name": ("description", "investment name", "security description", "name"),
    "value": ("mkt val", "market value", "current value", "total value", "value"),
    "basis": ("cost basis total", "cost basis", "total cost basis", "total cost", "cost"),
    "qty": ("qty", "quantity", "shares"),
    "asset": ("asset type", "security type", "asset class"),
    "acct_name": ("account name", "account type"),
    "acct_num": ("account number", "account"),
    "pct": ("percent of account", "of acct", "percent of acct"),
    "price": ("price", "last price", "share price"),
    "lot": ("acct type", "type"),            # Cash / Margin, per position (Chase, Fidelity)
    "as_of": ("as of",),
}
UNNAMED = "Account"                          # the label when the file does not name the account
_CASH_ROWS = {"cash & cash investments", "cash & money market", "cash and money market", "cash", "core position",
              "cash & sweep vehicle"}
_TOTAL_ROW = re.compile(r"^(positions|account|grand)?\s*totals?\b", re.I)
_PENDING = {"pending activity", "pending"}
_MONEY_FUND = re.compile(r"money\s*(market|mkt|fund)|\bmmf\b|cash reserves|govt? money", re.I)
_CUSIP = re.compile(r"^(?=.*\d)[0-9A-Z]{9}$")
_OPTION = re.compile(r"\d{2}/\d{2}/\d{4}|^-?[A-Z]{1,6}\d{6}[CP][\d.]+$|\s[\d.]+\s+[CP]$")
_MASK = re.compile(r"(?:\.\.\.|…|\*+|x{2,}|-)\s*([0-9A-Z]{3,4})\s*$", re.I)
_SCHWAB_TITLE = re.compile(r"^positions for\s+(?:account\s+)?(.*?)\s+as of\s+(.*)$", re.I)


class CannotImport(ValueError):
    """An export the page cannot read, or a sync it cannot carry out."""


# ═══════════════════════════════════════════════════════════════════
# READING THE EXPORT
# ═══════════════════════════════════════════════════════════════════

def _money(s) -> float | None:
    """'$1,234.56', '-$22,598.49', '($12.00)', '1,112.8238' → a number;
    'N/A', '--', '' → None."""
    t = str(s or "").strip().replace(",", "").replace("$", "").replace(" ", "")
    neg = t.startswith("(") and t.endswith(")")
    t = t.strip("()")
    if t.startswith("+"):
        t = t[1:]
    if not re.fullmatch(r"-?\d+(\.\d+)?", t):
        return None
    v = float(t)
    return -v if neg else v


def _norm(h: str) -> str:
    h = re.sub(r"\(.*?\)", "", str(h or "")).lower()
    return " ".join(re.sub(r"[^a-z0-9 ]", " ", h).split())


def _header_map(row: list[str]) -> dict | None:
    """{field: column index} for a header row, or None when the row is not
    a positions header (no symbol or no market value column)."""
    names = [_norm(c) for c in row]
    out = {}
    for field, cands in COLUMNS.items():
        for c in cands:
            if c in names:
                out[field] = names.index(c)
                break
    return out if "symbol" in out and "value" in out else None


def _looks_like_header(row: list[str]) -> bool:
    names = {_norm(c) for c in row}
    return bool(names & {"symbol", "ticker", "account number", "trade date", "settlement date"})


def _cells(row: list[str]) -> list[str]:
    return [c for c in (x.strip() for x in row) if c]


def mask_of(text: str) -> str | None:
    """The last digits of an account number, as the broker shows them
    ('Individual ...456' → '456'); a full number keeps only its last four."""
    t = str(text or "").strip()
    m = _MASK.search(t)
    if m and any(ch.isdigit() for ch in m.group(1)):
        return m.group(1).upper()
    digits = re.sub(r"[^0-9A-Z]", "", t.upper())
    return digits[-4:] if len(digits) >= 4 and any(ch.isdigit() for ch in digits) else None


def account_type(label: str) -> tuple[str, bool]:
    """(the holdings account, sure?) from the broker's account name."""
    l = f" {str(label or '').lower()} "
    if "health" in l or re.search(r"\bhsa\b", l):
        return "hsa", True
    plan = re.search(r"401|403|457|\btsp\b", l)
    window = is_window(l)
    if "roth" in l:
        return ("roth401k" if plan or window else "roth"), True
    if plan:
        return "k401", True
    if window:                       # inside a 401(k), but pre-tax or Roth it does not say
        return "k401", False
    if re.search(r"\bira\b|rollover|\bsep\b|simple|contributory|inherited|beneficiary ira", l):
        return "ira", True
    if re.search(r"individual|joint|trust|brokerage|custodial|ugma|utma|jtwros|tenants|tod\b|margin", l):
        return "taxable", True
    return "taxable", False


def is_window(label: str) -> bool:
    """A brokerage window inside a workplace plan (Fidelity BrokerageLink,
    Schwab PCRA, a self-directed brokerage account)."""
    return bool(re.search(r"brokeragelink|pcra|self[- ]directed", str(label or "").lower()))


def _as_of(text: str) -> str | None:
    """'04:05 PM ET, 2026/09/30' → '2026-09-30 16:05 ET'; any other text
    with a date in it → the date."""
    t = str(text or "")
    m = re.search(r"(\d{1,2}):(\d{2})\s*([AP])\.?M\.?\s*(ET|CT|PT|MT)?[, ]+(\d{4})/(\d{2})/(\d{2})", t, re.I)
    if m:
        h = int(m.group(1)) % 12 + (12 if m.group(3).upper() == "P" else 0)
        return f"{m.group(5)}-{m.group(6)}-{m.group(7)} {h:02d}:{m.group(2)}" + (f" {m.group(4).upper()}" if m.group(4) else "")
    m = re.search(r"(\d{4})[/-](\d{2})[/-](\d{2})", t)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    months = "jan feb mar apr may jun jul aug sep oct nov dec".split()
    m = re.search(r"\b(" + "|".join(months) + r")[a-z]*[- ](\d{1,2})[-, ]+(\d{4})"
                  r"(?:\s+(\d{1,2}):(\d{2})\s*([ap])\.?\s*m\.?\s*(ET|CT|MT|PT)?)?", t, re.I)
    if m:
        day = f"{m.group(3)}-{months.index(m.group(1)[:3].lower()) + 1:02d}-{int(m.group(2)):02d}"
        if not m.group(4):
            return day
        h = int(m.group(4)) % 12 + (12 if m.group(6).lower() == "p" else 0)
        return f"{day} {h:02d}:{m.group(5)}" + (f" {m.group(7).upper()}" if m.group(7) else "")
    m = re.search(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b", t)
    if m:
        return f"{m.group(3)}-{int(m.group(1)):02d}-{int(m.group(2)):02d}"
    return None


def _symbol(raw: str) -> str:
    s = str(raw or "").strip().upper().rstrip("*").strip()
    return s.replace("/", ".")


def _classify(sym: str, name: str, asset: str) -> str:
    """'cash' (the sweep), 'pending', 'option', 'fixed' (bonds, CDs),
    'mmf' (a money-market fund) or 'security'."""
    s, a = sym.lower(), (asset or "").lower()
    if s in _CASH_ROWS or (not sym and "cash" in a):
        return "cash"
    if s in _PENDING:
        return "pending"
    if "option" in a or _OPTION.search(sym):
        return "option"
    if re.search(r"fixed income|bond|\bcds?\b|treasur", a) or _CUSIP.match(sym):
        return "fixed"
    if _MONEY_FUND.search(name or "") or (sym and "money market" in a):
        return "mmf"
    return "security"


def _broker_of(headers: dict | None, title: str | None, filename: str, text: str = "") -> str:
    """Schwab's title line names it; otherwise the file name, otherwise the
    columns (Fidelity names the account, Vanguard only numbers it), otherwise
    Chase's footnotes, which name J.P. Morgan Securities."""
    f = (filename or "").lower()
    if title is not None:
        return "Schwab"
    for b in ("Schwab", "Fidelity", "Vanguard"):
        if b.lower() in f:
            return b
    if headers and "acct_name" in headers:
        return "Fidelity"
    if headers and "acct_num" in headers:
        return "Vanguard"
    if re.search(r"J\.\s?P\.\s?Morgan Securities", text or "") or "chase" in f:
        return "Chase"
    return "Broker"


def parse_export(text: str, filename: str = "") -> dict:
    """A positions export → {"broker", "as_of", "accounts": [...], "notes"}.
    Each account: key, label, broker, account (holdings account), sure,
    positions [{symbol, name, value, basis, qty, kind}], cash, margin,
    value (what the positions add up to), reported (the broker's own total,
    if it gives one), check ('ok' | 'off' | None), skipped [{what, value, why}]."""
    if text is None or not str(text).strip():
        raise CannotImport("The file is empty.")
    if len(text.encode("utf-8", "ignore")) > MAX_BYTES:
        raise CannotImport("That file is over 1 MB — a positions export is far smaller. Is it the right file?")
    text = str(text).lstrip("﻿")
    rows = list(csv.reader(io.StringIO(text)))
    title = None
    as_of = None
    pending_label = None
    header = None
    first_header = None
    accounts: dict[str, dict] = {}
    order: list[str] = []
    current = None                       # the account a Schwab section belongs to

    def account(label: str, num: str | None = None) -> dict:
        mask = mask_of(num) if num else mask_of(label)
        clean_label = re.sub(r"\s+", " ", (label or "").strip()) or UNNAMED
        if num and mask and mask not in clean_label:
            clean_label = f"{clean_label} …{mask}"
        clean_label = re.sub(r"\.\.\.\s*", "…", clean_label)
        key_tail = mask.lower() if mask else re.sub(r"[^a-z0-9]+", "-", clean_label.lower()).strip("-")[:20]
        k = key_tail
        if k not in accounts:
            typ, sure = account_type(label)
            accounts[k] = {"key": k, "mask": mask, "label": clean_label[:60], "account": typ, "sure": sure,
                           "positions": [], "margin_lots": False, "unnamed": clean_label == UNNAMED and not mask,
                           "cash": 0.0, "margin": 0.0, "reported": None, "skipped": []}
            order.append(k)
        return accounts[k]

    for row in rows:
        cells = _cells(row)
        if not cells:
            continue
        if len(cells) == 1:
            one = cells[0]
            m = _SCHWAB_TITLE.match(one)
            if m:
                title = one
                as_of = _as_of(m.group(2)) or as_of
                who = m.group(1).strip()
                if who.lower() not in ("all-accounts", "all accounts"):
                    current = account(who)
                header = None
                continue
            # a Schwab all-accounts section label, or a footer line: the table ends
            pending_label = one
            header = None
            if as_of is None and re.search(r"(as of|download|date)", one, re.I):
                as_of = _as_of(one)
            continue
        hm = _header_map(row)
        if hm:
            header = hm
            first_header = first_header or hm
            if pending_label and "acct_num" not in hm and "acct_name" not in hm:
                current = account(pending_label)
            pending_label = None
            continue
        if _looks_like_header(row):
            header = None                # another table in the file (Vanguard's trades)
            continue
        if header is None:
            continue

        def cell(field):
            i = header.get(field)
            return row[i].strip() if i is not None and i < len(row) else ""

        sym_raw = cell("symbol")
        if _TOTAL_ROW.match(sym_raw) or (not sym_raw and _TOTAL_ROW.match(cells[0])):
            tgt = None if "acct_num" in header else current     # Fidelity/Vanguard totals are not per account
            if tgt is not None:
                tgt["reported"] = _money(cell("value"))
            continue
        if "acct_num" in header or "acct_name" in header:
            num = cell("acct_num")
            nm = cell("acct_name")
            if not num and not nm:
                continue
            acct = account(nm or UNNAMED, num or None)
        else:
            acct = current or account(UNNAMED)
        if as_of is None and cell("as_of"):
            as_of = _as_of(cell("as_of"))
        if cell("lot").lower() == "margin":
            acct["margin_lots"] = True
        name = cell("name")
        sym = _symbol(sym_raw)
        kind = _classify(sym, name, cell("asset"))
        value = _money(cell("value"))
        if kind == "cash":
            if value is not None:
                if value < 0:
                    acct["margin"] += -value
                else:
                    acct["cash"] += value
            continue
        what = sym or name[:30] or "a line"
        if value is None:
            acct["skipped"].append({"what": what, "value": None, "why": "no market value in the file"})
            continue
        if kind == "pending":
            acct["skipped"].append({"what": "Pending activity", "value": value, "why": "not settled yet"})
            continue
        if kind == "option":
            acct["skipped"].append({"what": what, "value": value, "why": "an option — the board does not measure them"})
            continue
        if kind == "fixed":
            acct["skipped"].append({"what": (name or sym)[:40], "value": value,
                                    "why": "a bond or CD — add it by hand with its yield"})
            continue
        if kind == "security" and not A._TICKER_RE.match(sym):
            acct["skipped"].append({"what": what, "value": value, "why": "not a US ticker the board can read"})
            continue
        basis = _money(cell("basis"))
        prev = next((p for p in acct["positions"] if p["symbol"] == sym), None)
        if prev:                         # the same fund held as cash and on margin, say
            prev["value"] += value
            prev["basis"] = None if prev["basis"] is None or basis is None else prev["basis"] + basis
            prev["qty"] = None if prev["qty"] is None or _money(cell("qty")) is None else prev["qty"] + _money(cell("qty"))
            continue
        acct["positions"].append({"symbol": sym, "name": (name or sym)[:60], "value": value, "basis": basis,
                                  "qty": _money(cell("qty")), "kind": kind, "pct": _money(cell("pct").rstrip("%")),
                                  "price": _money(cell("price"))})

    broker = _broker_of(first_header, title, filename, text)
    out = []
    for k in order:
        a = accounts[k]
        if not a["positions"] and not a["cash"] and not a["margin"] and not a["skipped"]:
            continue
        if len(a["positions"]) > MAX_POSITIONS:
            raise CannotImport(f"{a['label']} has {len(a['positions'])} positions — more than the holdings box "
                               f"keeps ({MAX_POSITIONS}).")
        a["broker"] = broker
        a["key"] = f"{broker.lower()}-{a['key']}"
        if not a["sure"] and (a["margin"] > 0 or a.pop("margin_lots")):
            a["account"], a["sure"] = "taxable", True        # a margin account: retirement accounts cannot borrow
        a.pop("margin_lots", None)
        held = sum(p["value"] for p in a["positions"])
        a["value"] = round(held + a["cash"] - a["margin"], 2)
        skipped = sum(s["value"] or 0 for s in a["skipped"])
        a["check_from"], tol = "total", MATCH_TOLERANCE
        if a["reported"] is None:
            # no total row (Fidelity): the "% of account" column implies one —
            # from the largest line, good to the rounding of its percentage
            big = max((p for p in a["positions"] if p.get("pct")), key=lambda p: p["pct"], default=None)
            if big and big["pct"] > 0:
                a["reported"] = round(big["value"] / (big["pct"] / 100), 2)
                a["check_from"] = "percent"
                tol = max(MATCH_TOLERANCE, 2 * a["reported"] * 0.005 / big["pct"])
        priced = [p for p in a["positions"] if p.get("qty") is not None and p.get("price") is not None]
        if a["reported"] is None and a["positions"] and len(priced) == len(a["positions"]):
            # no total at all (Chase): each line's value must be its quantity × price
            bad = [p["symbol"] for p in priced
                   if abs(p["value"] - p["qty"] * p["price"]) > max(MATCH_TOLERANCE, 0.002 * abs(p["value"]))]
            a["check"], a["check_from"], a["check_bad"] = ("off" if bad else "ok"), "lines", bad
        elif a["reported"] is None:
            a["check"] = None
        else:
            a["check"] = "ok" if abs(a["value"] + skipped - a["reported"]) <= tol else "off"
            a["check_diff"] = round(a["reported"] - a["value"] - skipped, 2)
        a["window"] = is_window(a["label"])
        for f in ("cash", "margin"):
            a[f] = round(a[f], 2)
        out.append(a)
    if not out:
        raise CannotImport("No positions found. Export the Positions page as CSV (Schwab: Export, top right of "
                           "Positions; Fidelity: Download on Positions; Vanguard: Download on Balances & holdings).")
    if as_of is None:
        m = re.search(r"(\d{4})-(\d{2})-(\d{2})(?:-(\d{2})(\d{2})(\d{2}))?", filename or "")
        if m:
            as_of = f"{m.group(1)}-{m.group(2)}-{m.group(3)}" + (f" {m.group(4)}:{m.group(5)}" if m.group(4) else "")
    return {"broker": broker, "as_of": as_of, "accounts": out}


# ═══════════════════════════════════════════════════════════════════
# WRITING IT INTO THE HOLDINGS
# ═══════════════════════════════════════════════════════════════════

def block_lines(a: dict, as_of: str | None, p: dict) -> list[str]:
    """One account's positions as holdings lines between its sync markers."""
    acct = a["account"]
    rf = K._f(p.get("rf_rate"), 4.0)
    head = f"# sync {a['key']} | {a['broker']} · {a['label']} | " + (f"as of {as_of}" if as_of else "as exported")
    lines = [head]
    for pos in sorted(a["positions"], key=lambda r: -r["value"]):
        if pos["kind"] == "mmf":
            if acct == "taxable":
                lines.append(A.holding_line(pos["symbol"], "cash", pos["value"], rate=rf))
            else:
                lines.append(A.holding_line(f"{pos['symbol']} money fund", acct, pos["value"], rate=rf))
            continue
        basis = pos["basis"] if acct == "taxable" else None
        # the share count keeps the line at the live price between exports
        lines.append(A.holding_line(pos["symbol"], acct, pos["value"], basis=basis, qty=pos.get("qty")))
    if a["cash"] > 0:
        where = f"{a['broker']} …{a['mask']}" if a.get("mask") else a["broker"]
        if acct == "taxable":
            lines.append(A.holding_line(f"Cash at {where}", "cash", a["cash"], rate=0.0))
        else:
            lines.append(A.holding_line(f"Cash in {where}", acct, a["cash"], rate=0.0))
    lines.append(f"# end sync {a['key']}")
    return lines


def blocks_in(text: str) -> list[dict]:
    """The sync blocks in the holdings: [{key, header, start, end}] with the
    line numbers of their markers. An unterminated block runs to the end."""
    out, cur = [], None
    lines = str(text or "").splitlines()
    for n, raw in enumerate(lines):
        s = raw.strip()
        m = SYNC_START.match(s)
        if m and cur is None:
            cur = {"key": m.group(1).lower(), "header": m.group(2).strip(), "start": n, "end": None}
            continue
        m = SYNC_END.match(s)
        if m and cur is not None and m.group(1).lower() == cur["key"]:
            cur["end"] = n
            out.append(cur)
            cur = None
    if cur is not None:
        cur["end"] = len(lines) - 1
        out.append(cur)
    return out


def describe_block(b: dict) -> dict:
    """'Schwab · Individual …456 | as of 2026-09-30 16:05 ET' → its parts."""
    parts = [x.strip() for x in b["header"].split("|")]
    return {"key": b["key"], "label": parts[0] if parts else b["key"],
            "as_of": parts[1].replace("as of ", "") if len(parts) > 1 else None}


def manual_lines(text: str) -> list[dict]:
    """The holdings lines the owner typed (outside any sync block), parsed."""
    inside = set()
    for b in blocks_in(text):
        inside.update(range(b["start"], b["end"] + 1))
    rows, _ = A.parse_holdings(text)
    raw = str(text or "").splitlines()
    return [{**r, "text": raw[r["line"]].strip()} for r in rows if r["line"] not in inside]


def overlaps(text: str, accounts: list[dict]) -> list[dict]:
    """Typed lines in the account types being synced: a line naming a ticker
    the export now holds in that account type is checked for removal (it
    would count twice); the others are listed, unchecked."""
    types = {a["account"] for a in accounts}
    brokers = {a["broker"].lower() for a in accounts}
    held = {(a["account"], p["symbol"]) for a in accounts for p in a["positions"]}
    out = []
    for r in manual_lines(text):
        # a cash line only when it names the broker ("Schwab cash"), not checking
        at_broker = r["account"] == "cash" and any(b in r["name"].lower() for b in brokers)
        if r["account"] not in types and not at_broker:
            continue
        dup = (r["account"], (r.get("ticker") or "")) in held
        out.append({"text": r["text"], "name": r["name"], "account": r["account"], "value": r["value"],
                    "duplicate": dup})
    return out


def merge(text: str, blocks: dict[str, list[str]], remove: list[str] | None = None) -> str:
    """The holdings with each block put in place: an account synced before
    has its block replaced where it sits; a new one is added at the end.
    Typed lines whose text is in `remove` are dropped (outside blocks only)."""
    lines = str(text or "").replace("\r\n", "\n").splitlines()
    existing = {b["key"]: b for b in blocks_in(text)}
    drop = {s.strip() for s in (remove or []) if s and s.strip()}
    out, n = [], 0
    starts = {b["start"]: b for b in existing.values()}
    while n < len(lines):
        if n in starts:                  # a block is taken whole: nothing in it is "typed"
            b = starts[n]
            if b["key"] in blocks:
                out.extend(blocks[b["key"]])
            else:
                out.extend(lines[b["start"]:b["end"] + 1])
            n = b["end"] + 1
            continue
        if lines[n].strip() in drop:
            n += 1
            continue
        out.append(lines[n])
        n += 1
    for k, bl in blocks.items():
        if k not in existing:
            if out and out[-1].strip():
                out.append("")
            out.extend(bl)
    while out and not out[-1].strip():
        out.pop()
    new = "\n".join(out)
    if len(out) > A.MAX_LINES or len(new) > K._LINES["holdings"]:
        raise CannotImport(f"The holdings would run to {len(out)} lines — more than the {A.MAX_LINES} the page "
                           "reads. Leave an account out, or remove lines you no longer need.")
    return new


def margin_debt_name(a: dict) -> str:
    return (f"{a['broker']} margin …{a['mask']}" if a.get("mask") else f"{a['broker']} margin {a['label']}")[:40]


def slug(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", str(name or "").lower()).strip("-")[:20]


def named(a: dict, name) -> dict:
    """An account the file does not name, under the name the owner gives it —
    the same name next time puts the next export in the same group."""
    nm = re.sub(r"[^A-Za-z0-9 &'\-]", "", str(name or "")).strip()[:30]
    if not a.get("unnamed") or not nm or not slug(nm):
        return a
    return {**a, "label": nm, "key": f"{a['broker'].lower()}-{slug(nm)}"}


def apply_import(saved: dict, parsed: dict, choices: dict) -> tuple[dict, str]:
    """The profile with the chosen accounts synced → (profile, a one-line
    summary). choices: {"accounts": {key: {"include": bool, "account": str,
    "name": str}}, "margin_apr": {key: number}, "remove": [line text, ...]} —
    keyed by the key the preview showed."""
    picks = choices.get("accounts") if isinstance(choices.get("accounts"), dict) else {}
    aprs = choices.get("margin_apr") if isinstance(choices.get("margin_apr"), dict) else {}
    chosen = []
    for a in parsed["accounts"]:
        c = picks.get(a["key"]) if isinstance(picks.get(a["key"]), dict) else {}
        if c.get("include") is False:
            continue
        typ = A.account_of(c.get("account")) if c.get("account") else (a["account"], False)
        if typ is None or typ[0] in ("cash", "debt"):
            raise CannotImport(f"{a['label']}: choose the kind of account it is.")
        chosen.append({**named(a, c.get("name")), "account": typ[0], "_seen_as": a["key"]})
    if len({a["key"] for a in chosen}) < len(chosen):
        raise CannotImport("Two accounts were given the same name — give each its own.")
    if not chosen:
        raise CannotImport("No account chosen to sync.")
    p = K.profile_with_defaults(saved)
    blocks = {a["key"]: block_lines(a, parsed.get("as_of"), p) for a in chosen}
    remove = [s for s in choices.get("remove") or [] if isinstance(s, str)][:A.MAX_LINES]
    prof = dict(saved)
    prof["holdings"] = merge(saved.get("holdings") or "", blocks, remove)
    debts = [d for d in (saved.get("debts") or []) if isinstance(d, dict)]
    for a in chosen:
        name = margin_debt_name(a)
        debts = [d for d in debts if d.get("name") != name]
        if a["margin"] > 0:
            apr = K._f(aprs.get(a["_seen_as"]), None)
            if apr is None or not 0 <= apr <= 40:
                raise CannotImport(f"{a['label']} has a ${a['margin']:,.0f} margin loan — give its rate (APR).")
            debts.append({"name": name, "balance": a["margin"], "apr": apr})
    prof["debts"] = debts
    n = sum(len(a["positions"]) for a in chosen)
    names = ", ".join(f"{a['broker']} {a['label']}" for a in chosen)
    return prof, f"Synced {names} — {n} position{'s' if n != 1 else ''}"

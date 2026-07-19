"""Parse Golden 1 Credit Union PDF statements into a normalized ledger.

Golden 1 has no CSV export, so the household tool ingests the PDF the
member downloads. Two statement shapes are handled:

  • HELOC (credit-card style) — a Transactions table (charges positive,
    payments/credits negative) plus a summary box (balance, APR, minimum
    payment, interest charged, credit limit). We lift the summary so the
    decision system + renovation payoff need no manual entry.
  • Deposit statement (multi-account) — Savings / Checking activity
    tables (Post/Effective Date · Description · Withdrawals · Deposits ·
    Balance) plus any Loan sub-section (Amount · Finance Charge ·
    Principal · Balance).

Each parsed transaction carries a SIGNED amount in the ledger convention
(negative = money out of pocket, positive = money in), a cleaned/redacted
description and a bucket — the same shape normalize_rows() produces — so
PDF rows flow straight into the existing store/dashboard pipeline.
"""
from __future__ import annotations

import re

import household as H

_DATE = re.compile(r"^\d{2}/\d{2}/\d{4}$")
_AMT = re.compile(r"^-?\$?[\d,]+\.\d{2}$|^\$-[\d,]+\.\d{2}$")
_REF = re.compile(r"^[A-Z0-9]{12,}$")
_SKIP_DESC = ("beginning balance", "ending balance", "deposit(s) this period",
              "withdrawal(s) this period", "dividends", "annual percentage",
              "daily periodic", "total payments", "total withdrawals",
              "finance charges paid", "overdraft", "past due", "check(s) cleared")


def _money(s):
    if s is None:
        return None
    t = str(s).strip()
    neg = t.startswith("-") or t.startswith("$-")
    v = re.sub(r"[^0-9.]", "", t)
    if not v or v == ".":
        return None
    try:
        f = float(v)
    except ValueError:
        return None
    return -f if neg else f


def _pct(s):
    if s is None:
        return None
    v = re.sub(r"[^0-9.]", "", str(s))
    try:
        return float(v) if v else None
    except ValueError:
        return None


def parse(pdf_bytes: bytes) -> dict:
    """Parse a Golden 1 statement PDF. Returns
    {"type": "heloc"|"deposit", "accounts": [ {name, kind, transactions,
    summary} ]}. transactions are ledger dicts ready to store; summary
    (HELOC only) carries balance/apr/min_payment/interest/credit_limit.
    Raises RuntimeError if PyMuPDF isn't available or the PDF is unreadable."""
    try:
        import fitz  # PyMuPDF
    except Exception as e:  # pragma: no cover
        raise RuntimeError("PDF support needs PyMuPDF (add 'PyMuPDF' to requirements)") from e
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    except Exception as e:
        raise RuntimeError(f"could not open PDF: {e}") from e
    full = "\n".join(p.get_text() for p in doc)
    if "HELOC" in full or "HOME EQUITY LINE OF CREDIT" in full:
        return {"type": "heloc", "accounts": [_parse_heloc(full)]}
    return {"type": "deposit", "accounts": _parse_deposit(doc)}


# ── HELOC (credit-card style) ──────────────────────────────────────
def _parse_heloc(full: str) -> dict:
    lines = [ln.strip() for ln in full.split("\n")]

    def after(label):
        for i, l in enumerate(lines):
            if l == label and i + 1 < len(lines):
                return lines[i + 1]
        return None

    summary = {
        "balance": _money(after("New Balance")),
        "apr": _pct(after("Annual Percentage Rate")),
        "min_payment": _money(after("Minimum Payment Due:")),
        "interest": _money(after("Interest Charged")),
        "credit_limit": _money(after("Credit Limit")),
        "available": _money(after("Available Credit")),
    }

    txns = []
    try:
        start = next(i for i, l in enumerate(lines) if l == "Transactions")
    except StopIteration:
        start = None
    if start is not None:
        end = next((i for i, l in enumerate(lines)
                    if l.startswith("TOTAL INTEREST FOR THIS PERIOD")), len(lines))
        seg = lines[start:end]
        i = 0
        while i < len(seg):
            if _DATE.match(seg[i]) and i + 1 < len(seg) and _DATE.match(seg[i + 1]):
                date = seg[i]; i += 2
                mid = []
                while i < len(seg) and not _AMT.match(seg[i]) and not _DATE.match(seg[i]):
                    mid.append(seg[i]); i += 1
                if i < len(seg) and _AMT.match(seg[i]):
                    stmt_amt = _money(seg[i]); i += 1
                    if mid and _REF.match(mid[-1]):
                        mid = mid[:-1]
                    desc = " ".join(mid).strip()
                    if stmt_amt is not None and desc:
                        # statement: charge/advance positive → money out;
                        # payment/credit negative → money in. Ledger flips.
                        txns.append(_txn(date, desc, -stmt_amt))
                else:
                    continue
            else:
                i += 1
    txns = _dedupe(txns)
    return {"name": "Golden 1 HELOC", "kind": "heloc",
            "transactions": txns, "summary": summary}


# ── Deposit / loan statement (multi-account, column-aligned) ───────
def _rows(doc):
    """All words across pages as (page, y, [(x, text)...]) rows, grouped
    by rounded y so a printed table row reads left-to-right."""
    out = []
    for pno, page in enumerate(doc):
        buckets: dict[int, list] = {}
        for w in page.get_text("words"):
            x0, y0 = w[0], w[1]
            key = round(y0 / 3) * 3
            buckets.setdefault(key, []).append((x0, w[4]))
        for y in sorted(buckets):
            out.append((pno, y, sorted(buckets[y])))
    return out


_ACCT_HDR = re.compile(
    r"(Free Checking|Student Checking|Checking|Savings|Money Market|"
    r"Certificate|Club|Signature Loan|Personal Loan|Auto Loan|Loan)\s*\((\d{2})\)")


def _kind_of(name: str) -> str:
    n = name.lower()
    if "checking" in n:
        return "checking"
    if "savings" in n or "money market" in n or "club" in n or "certificate" in n:
        return "savings"
    if "loan" in n:
        return "loan"
    return "checking"


def _nearest(x, centers):
    return min(centers, key=lambda c: abs(c[0] - x))[1]


def _parse_deposit(doc) -> list[dict]:
    accounts: list[dict] = []
    cur = None            # current account dict
    cols = None           # ("deposit"|"loan", [(x, colname)...])
    last = None           # last emitted txn (for wrapped-description merge)
    for pno, y, cells in _rows(doc):
        joined = " ".join(t for _, t in cells).strip()
        low = joined.lower()

        m = _ACCT_HDR.search(joined)
        if m and "(continued)" not in low:
            name = f"Golden 1 {m.group(1)}"
            cur = {"name": name, "kind": _kind_of(name),
                   "transactions": [], "summary": {}, "_suffix": m.group(2)}
            accounts.append(cur)
            cols = None; last = None
            continue
        if cur is None:
            continue

        # header row of an activity table → learn the column x-centers
        if "post" in low and "date" in low and ("withdrawals" in low or "amount" in low):
            last = None
            centers = []
            style = "loan" if "principal" in low else "deposit"
            wanted = (["amount", "charge", "principal", "balance"] if style == "loan"
                      else ["withdrawals", "deposits", "balance"])
            for x, t in cells:
                tl = t.lower()
                for w in wanted:
                    if tl == w:
                        centers.append((x, w))
            if centers:
                cols = (style, centers)
            continue

        if cols is None:
            continue
        style, centers = cols
        # a data row starts with a date in the far-left column
        dates = [t for x, t in cells if x < 160 and _DATE.match(t)]
        if not dates:
            # A wrapped description continuation: no date, text in the
            # description column, no right-column dollar values. Fold it
            # into the previous txn so "...Transfer to loan 2" survives.
            has_col_amt = any(_AMT.match(t) and x >= 350 for x, t in cells)
            frag = " ".join(t for x, t in cells if 160 <= x < 350 and not _DATE.match(t)).strip()
            if last is not None and frag and not has_col_amt \
               and not any(k in frag.lower() for k in _SKIP_DESC):
                merged = (last["desc"] + " " + frag).strip()
                b = H.categorize(merged)
                last.update(desc=merged, bucket=b, cls=H.bucket_class(b),
                            mkey=H.merchant_key(merged))
            continue
        # Assign each right-side number to its column: among numbers within
        # tolerance of a column center, the real (right-aligned) value is the
        # RIGHTMOST — so a dollar amount embedded mid-description ("...200.00
        # to loan 2") isn't mistaken for the column value.
        nums = [(x, _money(t)) for x, t in cells if x >= 300 and _AMT.match(t)]
        assigned, used_x = {}, set()
        for cx, name in centers:
            cands = [(x, v) for x, v in nums if abs(x - cx) <= 45 and v is not None]
            if cands:
                bx, bv = max(cands, key=lambda p: p[0])
                assigned[name] = bv; used_x.add(bx)
        # Description = everything from the description column onward that
        # ISN'T a claimed column value (keeps long descriptions + the
        # "to loan 2" tail that drives transfer detection).
        desc = " ".join(t for x, t in cells
                        if x >= 160 and not (_AMT.match(t) and x in used_x)).strip()
        low_desc = desc.lower()
        is_summary_line = any(k in low_desc for k in _SKIP_DESC)
        if assigned.get("balance") is not None:
            cur["summary"]["balance"] = assigned["balance"]
        if not desc or is_summary_line:
            last = None
            continue
        date = dates[0]
        last = None
        if style == "deposit":
            amt = None
            if assigned.get("withdrawals") is not None:
                amt = -abs(assigned["withdrawals"])
            elif assigned.get("deposits") is not None:
                amt = abs(assigned["deposits"])
            if amt is not None:
                last = _txn(date, desc, amt)
                cur["transactions"].append(last)
        else:  # loan section
            charge = assigned.get("charge") or 0
            pay = assigned.get("amount") or 0
            if pay:
                cur["transactions"].append(_txn(date, "Loan payment " + desc, -abs(pay),
                                                bucket="Loan Payment"))
            if charge:
                cur["transactions"].append(_txn(date, "Interest charge", -abs(charge),
                                                bucket="Fees & Interest"))

    for a in accounts:
        a.pop("_suffix", None)
        a["transactions"] = _dedupe(a["transactions"])
    # Drop empty accounts (near-zero savings with no activity) so the
    # import UI isn't cluttered with dormant suffixes.
    return [a for a in accounts if a["transactions"] or a["summary"].get("balance")]


# ── shared ─────────────────────────────────────────────────────────
def _txn(raw_date, raw_desc, amount, bucket=None):
    date = H._norm_date(raw_date)
    desc = H.clean_desc(raw_desc)
    b = bucket or H.categorize(desc)
    return {
        "date": date, "desc": desc, "amount": round(amount, 2),
        "bucket": b, "cls": H.bucket_class(b), "mkey": H.merchant_key(desc),
        "hash": H._row_hash(date or "", amount, desc),
    }


def _dedupe(txns):
    """Same within-batch hash disambiguation normalize_rows() uses, and
    drop rows that failed to get a date."""
    out = [t for t in txns if t["date"]]
    seen: dict[str, int] = {}
    for t in out:
        n = seen.get(t["hash"], 0)
        seen[t["hash"]] = n + 1
        if n:
            t["hash"] = f"{t['hash']}{n}"
    return out

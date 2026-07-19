"""Household finance engine — /household (alias /casa).

Turns raw bank/card CSV exports into one clean, categorized ledger so a
non-technical person can see "what got spent on what." Built first
against real Golden 1 statements (multi-account: checking + savings +
loans), but the CSV path is institution-agnostic via a per-account
column mapping.

Design principles (see the design memo in chat):
  1. ONE ledger from many accounts. Every row normalizes to a signed
     amount — NEGATIVE = money out, POSITIVE = money in — regardless of
     how the source encodes it.
  2. Transfers are NOT spending. Moving money between her own accounts,
     and paying a credit card (whose purchases are counted on the card
     side), must be excluded or the total double-counts. Golden 1 self-
     labels these ("...Transfer 'DTL' to loan 2", "...from share 9"),
     which makes them easy to catch.
  3. Categorize by rules, not guesswork. A keyword table auto-sorts most
     rows; the rest go to a review tray and, once assigned, become a
     learned rule so next month is automatic.
  4. Privacy: descriptions are redacted (account/card/SSN digit runs
     stripped) BEFORE anything is stored. Raw files are never persisted.

Pure logic, no I/O or DB — unit-testable in isolation.
"""
from __future__ import annotations

import csv
import io
import re
from collections import defaultdict

# ── Bucket taxonomy ────────────────────────────────────────────────
# Each bucket has a CLASS that drives the dashboard math:
#   income   → Money In
#   fixed    → recurring obligations (housing, utilities, insurance)
#   variable → discretionary / everyday spending
#   debt     → interest & fees paid (a real cost of carrying debt)
#   transfer → internal moves + credit-card payments — EXCLUDED from spend
#   review   → not yet categorized
BUCKET_CLASS = {
    "Income": "income",
    "Mortgage": "fixed",
    "HELOC": "fixed",
    "Rent": "fixed",
    "Utilities": "fixed",
    "Phone & Internet": "fixed",
    "Insurance": "fixed",
    "Groceries": "variable",
    "Dining": "variable",
    "Gas & Fuel": "variable",
    "Auto & Transport": "variable",
    "Health & Pharmacy": "variable",
    "Shopping": "variable",
    "Household Goods": "variable",
    "Home Improvement": "variable",
    "Furniture": "variable",
    "Clothes": "variable",
    "Beauty": "variable",
    "Subscriptions": "variable",
    "Entertainment": "variable",
    "Travel": "variable",
    "Kids & School": "variable",
    "Cash & ATM": "variable",
    "Other": "variable",
    "Fees & Interest": "debt",
    "Debt Payment": "debt",
    "Credit Card Payment": "transfer",
    "Transfer": "transfer",
    "Uncategorized": "review",
}

# Ordered rules — FIRST match wins, so put the specific/transfer rules
# before the broad merchant buckets. Each entry: (bucket, [keywords]).
# Keywords match case-insensitively as substrings of the cleaned desc.
DEFAULT_RULES: list[tuple[str, list[str]]] = [
    # ── Interest first — "Interest Charge on Cash Advances" must land in
    # Fees & Interest, not get grabbed by the "cash advance" transfer kw. ──
    ("Fees & Interest", ["finance charge", "interest charge", "interest chg"]),
    # ── Person-to-person apps are real payments to people (here, mostly
    # renovation labor — "ZELLE ... TO ROSALES"), NOT internal transfers.
    # They must NOT be excluded like a transfer, so catch them before the
    # Transfer rule grabs the word "transfer" and leave them for review /
    # reno-tagging. (Internal Zelle to your own accounts is rare.) ──
    ("Uncategorized", ["zelle", "venmo", "cash app", "cashapp"]),
    # ── Internal transfers (Golden 1 self-labels these) ──
    ("Transfer", [
        "transfer", "to loan", "from loan", "to share", "from share",
        "online transfer", "moneylink", "overdraft protection",
        "book transfer", "internal transfer", "to savings", "from savings",
        "mobile deposit transfer", "cash advance", "schwab", "brokerage",
    ]),
    # ── Credit-card payments (counted on the card side, so exclude here) ──
    ("Credit Card Payment", [
        "payment thank you", "thank you", "autopay", "online payment", "epay",
        "cardmember serv", "card payment", "credit card pmt", "cc pymt",
        "chase card", "amex epayment", "american express ach",
        "capital one crcardpmt", "discover e-pymt", "citi card",
        "citi autopay", "synchrony", "barclaycard", "bk of amer",
        "bofa cc", "visa payment", "mastercard payment", "bill pay",
    ]),
    # ── Income ──
    ("Income", [
        "payroll", "direct dep", "dir dep", "dirdep", "edd", "unemploy",
        "ssa treas", "soc sec", "social security", "pension", "annuity",
        "irs treas", "tax ref", "state tax", "franchise tax bd",
        "gusto", "adp", "paychex", "workday", "salary", "wages",
        "deposit dividend", "interest paid",
        "caltrans", "sco ", "state controller", "calstrs", "calpers",  # CA state pay/COLA
    ]),
    # ── Housing (fixed) ──
    ("Mortgage", [
        "mortgage", "mtg", "home loan", "loan servic", "mr cooper",
        "rocket mortgage", "loancare", "pennymac", "penny mac",
        "freedom mortgage", "caliber home", "wells fargo home",
        "chase mortgage", "flagstar", "newrez", "shellpoint", "carrington",
        "loandepot", "loan depot",
    ]),
    ("HELOC", [
        "heloc", "home equity", "equity line", "line of credit", "eq line",
    ]),
    ("Rent", ["rent ", "rental pmt", "property mgmt", "apartments", "leasing"]),
    ("Utilities", [
        "pg&e", "pge ", "pacific gas", "edison", "so cal edison", "sce ",
        "smud", "ebmud", "water dist", "water util", "east bay mud",
        "recology", "waste mgmt", "waste management", "republic services",
        "sewer", "sanitation", "garbage", "utility", "utilities",
        "dominion energy", "socalgas", "so cal gas",
    ]),
    ("Phone & Internet", [
        "comcast", "xfinity", "at&t", "att ", "verizon", "t-mobile",
        "tmobile", "spectrum", "sonic.net", "google fiber", "internet",
        "wireless", "cricket", "mint mobile", "metropcs", "frontier comm",
    ]),
    ("Insurance", [
        "insurance", "geico", "state farm", "allstate", "farmers ins",
        "progressive", "aaa ", "csaa", "mercury ins", "metlife",
        "libertymutual", "liberty mutual", "nationwide", "the general",
    ]),
    # ── Everyday spending (variable) ──
    ("Groceries", [
        "safeway", "trader joe", "whole foods", "wholefds", "sprouts",
        "raley", "lucky super", "foodmaxx", "food maxx", "grocery outlet",
        "aldi", "kroger", "smart & final", "smart final", "nob hill",
        "grocery", "supermarket", "cardenas", "mi pueblo", "food 4 less",
        # warehouse stores are mostly food/household staples → essential.
        # (specific "whse/wholesale" so it never catches "COSTCO GAS")
        "costco whse", "costco wholesale", "sam's club", "sams club", "bj's whs",
    ]),
    ("Dining", [
        "restaurant", " cafe", "coffee", "starbucks", "peet", "mcdonald",
        "taco", "pizza", "grill", "kitchen", "doordash", "uber eats",
        "ubereats", "grubhub", "chipotle", "panera", "subway", "chick-fil",
        "in-n-out", "in n out", "jack in the box", "burger", "sushi",
        "thai", "chinese rest", "deli", "bakery", "donut", "tst*", "sq *",
    ]),
    ("Gas & Fuel", [
        "chevron", "shell oil", "shell service", " arco", "76 -", "valero",
        "exxon", "mobil", "gas station", "fuel", "costco gas", "gasoline",
        "conoco", "phillips 66", "circle k", "sunoco", "speedway",
    ]),
    ("Auto & Transport", [
        "dmv", "bart", "clipper", "fastrak", "fas trak", "bridge toll",
        "toll ", "parking", "auto repair", "jiffy lube", "smog", "uber ",
        "lyft", "car wash", "tire", "autozone", "o'reilly auto", "napa auto",
    ]),
    ("Health & Pharmacy", [
        "cvs", "walgreens", "pharmacy", "kaiser", "sutter health", "dignity",
        "dental", "dentist", "medical", "clinic", "hospital", "optometr",
        "vision care", "urgent care", "labcorp", "quest diag", "rx ",
        "blue shield", "anthem", "health net", "goodrx",
    ]),
    # ── Home Improvement — she's mid-reno; keep hardware/paint/flooring
    # out of generic Shopping so the renovation picture is clean. Comes
    # before Shopping so "home depot" lands here, and these also auto-tag
    # to the reno on import (see RENO_VENDORS). ──
    ("Home Improvement", [
        "home depot", "homedepot", "lowe's", "lowes", "ace hardware",
        "orchard supply", "harbor freight", "floor & decor", "floor and decor",
        "ferguson", "sherwin", "benjamin moore", "creative paint", "the tile",
        "tile shop", "hardware store", "lumber", "build.com", "menards",
        "paint #", "paint co", "glass & mirror", "kitchen & bath",
    ]),
    ("Furniture", [
        "furniture", "ashley homestore", "ashley furniture", "wayfair",
        "west elm", "pottery barn", "crate & barrel", "crate and barrel",
        "cb2", "living spaces", "la-z-boy", "lazboy", "room & board",
        "mattress", "ikea", "ethan allen", "restoration hardware",
        "arhaus", "bassett", "z gallerie", "interior define",
    ]),
    ("Clothes", [
        "nordstrom", "macy", "old navy", "banana republic", "j.crew",
        "j crew", "h&m", "h & m", "zara", "uniqlo", "nike", "adidas",
        "lululemon", "ross store", "ross dress", "tj maxx", "tjmaxx",
        "marshalls", "kohl", "the gap", "gap store", "dsw", "foot locker",
        "shoe", "clothing", "apparel", "boutique", "athleta", "forever 21",
    ]),
    ("Beauty", [
        "sephora", "ulta", "sally beauty", "bath & body", "bath and body",
        "cosmetic", "salon", "day spa", "nail salon", "mac cosmetics",
        "lush ", "sports clips", "supercuts", "barber", "hair studio",
    ]),
    ("Household Goods", [
        "target", "walmart", "wal-mart", "bed bath", "container store",
        "homegoods", "home goods", "at home", "dollar tree", "dollar general",
        "big lots", "world market", "tuesday morning", "the container",
    ]),
    ("Shopping", [
        "amazon", "amzn", "best buy", "home depot", "lowe's", "lowes",
        "etsy", "ebay", "target.com",
    ]),
    ("Subscriptions", [
        "netflix", "spotify", "hulu", "disney plus", "disney+", "apple.com",
        "google *", "google one", "youtube", "amazon prime", "prime video",
        "audible", "nytimes", "new york times", "hbo", "max.com", "paramount+",
        "peacock", "patreon", "adobe", "microsoft", "msft", "icloud",
        "dropbox", "linkedin", "chatgpt", "openai", "1password", "notion",
    ]),
    ("Entertainment", [
        "cinema", "amc ", "regal", "cinemark", "theater", "theatre",
        "movie", "steam games", "playstation", "xbox", "nintendo",
        "ticketmaster", "stubhub", "concert", "fandango", "dave & buster",
    ]),
    ("Travel", [
        "airline", "united air", "southwest", "delta air", "american air",
        "alaska air", "hotel", "marriott", "hilton", "airbnb", "expedia",
        "booking.com", "hertz", "enterprise rent", "amtrak", "rental car",
    ]),
    ("Kids & School", [
        "school", "tuition", "university", "college", "daycare", "child care",
        "kindercare", "ymca", "tutoring", "scholastic", "campus",
    ]),
    ("Cash & ATM", ["atm ", "atm withdrawal", "cash withdrawal", "withdrawal cash"]),
    # ── Debt cost ──
    ("Fees & Interest", [
        "finance charge", "interest charge", "interest chg", "overdraft fee",
        "late fee", "annual fee", "service fee", "nsf fee", "returned item",
        "foreign transaction fee", "atm fee", "monthly fee", "maintenance fee",
    ]),
    ("Debt Payment", [
        "loan pmt", "loan payment", "signature loan", "student loan",
        "sallie mae", "nelnet", "great lakes", "navient", "auto loan",
    ]),
]


# ── Cleaning & redaction ───────────────────────────────────────────
_PREFIXES = [
    "pos purchase", "pos debit", "debit card purchase", "debit purchase",
    "checkcard", "check card", "purchase authorized on", "purchase auth",
    "recurring payment", "external withdrawal", "external deposit",
    "ach debit", "ach credit", "ach withdrawal", "ach deposit",
    "withdrawal", "deposit", "point of sale", "visa dda", "card purchase",
]
_SSN = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")
_LONG_DIGITS = re.compile(r"\b[\dxX*]{5,}\b")           # acct/card numbers
_ENDING_IN = re.compile(r"(ending in|acct|account|card|x{2,}|#)\s*[\dxX*]{3,}", re.I)
_REF = re.compile(r"\b(ref|trace|id|auth|conf|seq)[#:]?\s*\w{4,}", re.I)
_WS = re.compile(r"\s+")


def redact(raw: str) -> str:
    """Strip anything account-identifying from a description BEFORE it's
    stored: SSNs, long digit/masked runs (card & account numbers),
    "ending in 1234", reference/trace ids. Merchant names survive."""
    s = raw or ""
    s = _SSN.sub("•••", s)
    s = _ENDING_IN.sub("", s)
    s = _REF.sub("", s)
    s = _LONG_DIGITS.sub("", s)
    return _WS.sub(" ", s).strip(" -•·")


def clean_desc(raw: str) -> str:
    """Human-friendly, redacted, prefix-stripped description used for
    both display and rule-matching."""
    s = redact(raw)
    low = s.lower()
    for p in _PREFIXES:
        if low.startswith(p):
            s = s[len(p):].lstrip(" -:*")
            low = s.lower()
    # collapse a leading date fragment banks sometimes prepend
    s = re.sub(r"^\d{1,2}[/-]\d{1,2}(?:[/-]\d{2,4})?\s+", "", s)
    return _WS.sub(" ", s).strip() or (raw or "").strip()


def merchant_key(desc: str) -> str:
    """A stable key for grouping the same payee across months (for
    recurring detection). Lowercased, alnum-only, first ~24 chars."""
    k = re.sub(r"[^a-z0-9 ]", "", (desc or "").lower())
    k = _WS.sub(" ", k).strip()
    return k[:24]


# ── Categorization ─────────────────────────────────────────────────
def categorize(desc: str, learned: dict[str, str] | None = None) -> str:
    """Return the bucket for a cleaned description. Learned rules (a
    {keyword: bucket} map the user has confirmed) win over the built-in
    table so her corrections stick. Falls back to 'Uncategorized'."""
    low = (desc or "").lower()
    if learned:
        # longest keyword first so a specific override beats a general one
        for kw in sorted(learned, key=len, reverse=True):
            if kw and kw in low:
                return learned[kw]
    for bucket, kws in DEFAULT_RULES:
        for kw in kws:
            if kw in low:
                return bucket
    return "Uncategorized"


def bucket_class(bucket: str) -> str:
    return BUCKET_CLASS.get(bucket, "variable")


def is_transfer(bucket: str) -> bool:
    return bucket_class(bucket) == "transfer"


# ── CSV parsing / column mapping ───────────────────────────────────
_DATE_HDRS = ("post date", "posted date", "transaction date", "trans date",
              "date", "posting date", "effective date")
_DESC_HDRS = ("description", "payee", "memo", "name", "merchant",
              "transaction", "details", "narrative")
_AMT_HDRS = ("amount", "transaction amount", "amt")
_DEBIT_HDRS = ("debit", "withdrawal", "withdrawals", "withdrawal ($)",
               "money out", "outflow", "charges")
_CREDIT_HDRS = ("credit", "deposit", "deposits", "deposit ($)",
                "money in", "inflow", "payments")
_BAL_HDRS = ("balance", "balance ($)", "running balance", "current balance", "ending balance")
_FEE_HDRS = ("finance charge", "interest charge", "interest ($)", "finance charge ($)")


def _find(headers_low: list[str], candidates) -> int | None:
    # exact match first, then substring
    for c in candidates:
        for i, h in enumerate(headers_low):
            if h == c:
                return i
    for c in candidates:
        for i, h in enumerate(headers_low):
            if c in h:
                return i
    return None


def parse_csv(text: str) -> tuple[list[str], list[list[str]]]:
    """Return (headers, rows). Tolerates BOMs and blank trailing lines."""
    text = (text or "").lstrip("﻿")
    reader = csv.reader(io.StringIO(text))
    rows = [r for r in reader if any((c or "").strip() for c in r)]
    if not rows:
        return [], []
    return rows[0], rows[1:]


def auto_detect_mapping(headers: list[str], sample: list[list[str]]) -> dict:
    """Guess the column mapping for a bank/card CSV. Returns a dict the
    UI shows for confirmation and that normalize_rows() consumes:
        {date, desc, amount, debit, credit, mode}
    mode ∈ {signed, debit_credit}. For 'signed', sign_out records which
    sign means money-out (-1 typical for checking; +1 for many cards)."""
    low = [(h or "").strip().lower() for h in headers]
    m: dict = {
        "date": _find(low, _DATE_HDRS),
        "desc": _find(low, _DESC_HDRS),
        "amount": None, "debit": None, "credit": None,
        "mode": "signed", "sign_out": -1,
        # Loan/HELOC statements carry a running balance + a separate
        # finance-charge column; capturing them lets us read her HELOC
        # balance and interest straight from the statement.
        "balance": _find(low, _BAL_HDRS),
        "fee": _find(low, _FEE_HDRS),
    }
    debit, credit = _find(low, _DEBIT_HDRS), _find(low, _CREDIT_HDRS)
    amount = _find(low, _AMT_HDRS)
    if debit is not None and credit is not None:
        m["mode"] = "debit_credit"
        m["debit"], m["credit"] = debit, credit
    elif amount is not None:
        m["mode"] = "signed"
        m["amount"] = amount
        # Infer which sign means "out" from the sample: checking exports
        # skew negative (more/larger outflows than deposits shown neg).
        vals = []
        for r in sample[:40]:
            if amount < len(r):
                v = _to_float(r[amount])
                if v is not None and v != 0:
                    vals.append(v)
        negs = sum(1 for v in vals if v < 0)
        m["sign_out"] = -1 if negs >= max(1, len(vals) - negs) else 1
    return m


_NUM = re.compile(r"[^\d.\-()]")


def _to_float(s):
    """Parse '$1,234.56', '(12.30)', '-5', '' → float or None."""
    if s is None:
        return None
    t = str(s).strip()
    if not t or t in ("-", "--"):
        return None
    neg = t.startswith("(") and t.endswith(")")
    t = _NUM.sub("", t)
    if t in ("", "-", "."):
        return None
    try:
        v = float(t)
    except ValueError:
        return None
    return -v if neg else v


def normalize_rows(headers: list[str], rows: list[list[str]], mapping: dict,
                   learned: dict[str, str] | None = None) -> list[dict]:
    """Turn raw CSV rows into ledger dicts with a SIGNED amount
    (negative = out, positive = in), a cleaned+redacted description, and
    a categorized bucket. Rows without a usable date or amount are
    skipped. Deduped by (date, amount, desc) hash key set on each dict."""
    out = []
    di, si = mapping.get("date"), mapping.get("desc")
    mode = mapping.get("mode", "signed")
    for r in rows:
        def cell(idx):
            return r[idx] if (idx is not None and idx < len(r)) else ""
        raw_desc = cell(si)
        date = _norm_date(cell(di))
        if mode == "debit_credit":
            debit = _to_float(cell(mapping.get("debit")))
            credit = _to_float(cell(mapping.get("credit")))
            amt = (credit or 0.0) - (debit or 0.0)   # in − out
        else:
            v = _to_float(cell(mapping.get("amount")))
            if v is None:
                amt = None
            else:
                # sign_out == -1 means source already uses negative for out.
                amt = v if mapping.get("sign_out", -1) == -1 else -v
        if date is None or amt is None:
            continue
        desc = clean_desc(raw_desc)
        bucket = categorize(desc, learned)
        out.append({
            "date": date,
            "desc": desc,
            "amount": round(amt, 2),
            "bucket": bucket,
            "cls": bucket_class(bucket),
            "mkey": merchant_key(desc),
            "hash": _row_hash(date, amt, desc),
        })
        # Loan/HELOC statements: the interest lives in a separate finance-
        # charge column on the payment row. Emit it as its own Fees &
        # Interest line so it shows in the ledger and the debt true-cost.
        fee = _to_float(cell(mapping.get("fee"))) if mapping.get("fee") is not None else None
        if fee and fee > 0:
            out.append({
                "date": date, "desc": "Interest charge",
                "amount": round(-fee, 2), "bucket": "Fees & Interest",
                "cls": "debt", "mkey": "interest charge",
                "hash": _row_hash(date, -fee, "interest " + date),
            })
    # Disambiguate identical rows within this batch (e.g. two $7.85 coffees
    # the same day) so dedupe-on-hash doesn't drop legitimate repeats —
    # while re-importing the SAME file still reproduces the same hashes
    # (idempotent) because order is stable.
    seen: dict[str, int] = {}
    for t in out:
        n = seen.get(t["hash"], 0)
        seen[t["hash"]] = n + 1
        if n:
            t["hash"] = f"{t['hash']}{n}"
    return out


_DATE_FMTS = (
    "%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d", "%m-%d-%Y", "%d/%m/%Y",
    "%b %d, %Y", "%b %d %Y", "%m/%d",
)


def _norm_date(s: str) -> str | None:
    """Normalize a date cell to ISO YYYY-MM-DD (string). Returns None if
    unparseable. Uses only format parsing (no 'now') so it's pure."""
    from datetime import datetime
    t = (s or "").strip()
    if not t:
        return None
    for fmt in _DATE_FMTS:
        try:
            d = datetime.strptime(t, fmt)
            if fmt == "%m/%d":          # no year in source — leave year 1900
                return None             # unusable without a year; skip
            return d.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _row_hash(date: str, amt: float, desc: str) -> str:
    import hashlib
    key = f"{date}|{round(amt, 2)}|{merchant_key(desc)}"
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]


# ── Recurring detection ────────────────────────────────────────────
def find_recurring(txns: list[dict]) -> list[dict]:
    """A payee is 'recurring' if it appears as an OUTFLOW in ≥2 distinct
    months at a roughly stable amount (within 20%). Returns one row per
    recurring payee with its typical amount and month count — the basis
    for the subscriptions / fixed-bills view."""
    groups: dict[str, list[dict]] = defaultdict(list)
    for t in txns:
        if t["amount"] < 0 and t["cls"] not in ("transfer",) and t["mkey"]:
            groups[t["mkey"]].append(t)
    out = []
    for mkey, ts in groups.items():
        months = {t["date"][:7] for t in ts}
        if len(months) < 2:
            continue
        amts = sorted(abs(t["amount"]) for t in ts)
        typical = amts[len(amts) // 2]                  # median
        if typical <= 0:
            continue
        stable = sum(1 for a in amts if abs(a - typical) <= 0.20 * typical)
        if stable < 2:
            continue
        out.append({
            "merchant": ts[0]["desc"],
            "bucket": ts[0]["bucket"],
            "typical": round(typical, 2),
            "months": len(months),
            "count": len(ts),
        })
    out.sort(key=lambda r: r["typical"] * r["months"], reverse=True)
    return out


def fixed_bills(txns):
    """Her committed monthly nut: the recurring FIXED and DEBT outflows —
    mortgage, utilities, insurance, phone, subscriptions, loan/HELOC/card
    payments — grouped by payee with the typical monthly amount. Uses every
    month present (median), flags which are seen in ≥2 months, and rolls up
    a per-category total."""
    groups: dict[str, list[dict]] = defaultdict(list)
    for t in txns:
        if t["amount"] < 0 and bucket_class(t["bucket"]) in ("fixed", "debt"):
            key = t.get("mkey") or merchant_key(t.get("desc", ""))
            if key:
                groups[key].append(t)
    bills = []
    for mkey, ts in groups.items():
        months = sorted({t["date"][:7] for t in ts if t.get("date")})
        amts = sorted(abs(t["amount"]) for t in ts)
        typical = amts[len(amts) // 2] if amts else 0.0
        if typical <= 0:
            continue
        bills.append({
            "merchant": ts[-1]["desc"], "bucket": ts[0]["bucket"],
            "typical": round(typical, 2), "months": len(months),
            "recurring": len(months) >= 2,
        })
    # recurring first, then by size
    bills.sort(key=lambda b: (b["recurring"], b["typical"]), reverse=True)
    by_bucket: dict[str, float] = defaultdict(float)
    for b in bills:
        by_bucket[b["bucket"]] += b["typical"]
    sections = [{"bucket": k, "amount": round(v, 2)}
                for k, v in sorted(by_bucket.items(), key=lambda x: -x[1])]
    return {
        "bills": bills, "sections": sections,
        "total": round(sum(b["typical"] for b in bills), 2),
        "count": len(bills),
    }


def top_merchants(txns, limit=12):
    """Where the money actually goes, by payee — biggest first. Every outflow
    (transfers and income excluded) grouped by merchant with the total,
    typical monthly and count, so Costco, the gas stations, etc. surface on
    their own instead of hiding inside a bucket."""
    groups: dict[str, list[dict]] = defaultdict(list)
    for t in txns:
        if t["amount"] < 0 and t["cls"] not in ("transfer", "income"):
            key = t.get("mkey") or merchant_key(t.get("desc", ""))
            if key:
                groups[key].append(t)
    out = []
    for _, ts in groups.items():
        total = round(sum(-t["amount"] for t in ts), 2)
        months = len({t["date"][:7] for t in ts if t.get("date")}) or 1
        out.append({
            "merchant": ts[-1]["desc"], "bucket": ts[0]["bucket"],
            "total": total, "monthly": round(total / months, 2),
            "count": len(ts), "months": months,
        })
    out.sort(key=lambda r: r["total"], reverse=True)
    return out[:limit]


def spend_lookup(txns, query):
    """Total spend at a store or category matching `query` — matches the
    description OR the bucket, so 'costco', 'gas', 'geico' all work. Returns
    the total, the typical month, and a sample of the actual charges so she
    can see exactly what's counted."""
    q = (query or "").strip().lower()
    empty = {"query": query, "total": 0.0, "monthly": 0.0, "count": 0,
             "months": 0, "matches": []}
    if not q:
        return empty
    hits = [t for t in txns
            if t["amount"] < 0 and t["cls"] not in ("transfer", "income")
            and (q in (t.get("desc") or "").lower() or q in (t.get("bucket") or "").lower())]
    if not hits:
        return empty
    total = round(sum(-t["amount"] for t in hits), 2)
    months = len({t["date"][:7] for t in hits if t.get("date")}) or 1
    sample = sorted(hits, key=lambda t: t.get("date") or "", reverse=True)[:8]
    # split by bucket so a fuzzy word ('gas' also hits 'Pacific Gas & El')
    # is shown honestly — utilities vs. actual fuel, not one lump.
    by_bucket: dict[str, float] = defaultdict(float)
    for t in hits:
        by_bucket[t["bucket"]] += -t["amount"]
    return {
        "query": query, "total": total, "monthly": round(total / months, 2),
        "count": len(hits), "months": months,
        "by_bucket": [{"bucket": k, "amount": round(v, 2)}
                      for k, v in sorted(by_bucket.items(), key=lambda x: -x[1])],
        "matches": [{"date": t["date"], "desc": t["desc"],
                     "amount": round(t["amount"], 2), "bucket": t["bucket"]}
                    for t in sample],
    }


def _month_range(start, end):
    """All YYYY-MM strings from start to end inclusive."""
    out = []
    y, m = (int(x) for x in start.split("-"))
    ey, em = (int(x) for x in end.split("-"))
    while (y, m) <= (ey, em):
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            m = 1
            y += 1
    return out


def statement_coverage(txns, accounts):
    """Per account, which months have transactions and which months are
    MISSING inside that range — so a skipped statement (a HELOC month you
    forgot) stands out instead of hiding. Returns one row per account."""
    by_acct: dict = defaultdict(set)
    for t in txns:
        aid = t.get("account_id")
        if t.get("date") and aid is not None:
            by_acct[aid].add(t["date"][:7])
    out = []
    for a in (accounts or []):
        months = sorted(by_acct.get(a["id"], set()))
        gaps = []
        if len(months) >= 2:
            present = set(months)
            gaps = [mo for mo in _month_range(months[0], months[-1]) if mo not in present]
        out.append({
            "id": a["id"], "name": a["name"], "kind": a.get("kind"),
            "months": months, "gaps": gaps,
            "first": months[0] if months else None,
            "last": months[-1] if months else None,
            "count": len(months),
        })
    return out


def income_streams(txns, labels=None):
    """Her income broken into streams — payroll, a nursing pension, a state
    COLA, etc. — grouped by payee, each with its typical monthly amount and
    the months it's appeared. `labels` is a {merchant_key: friendly name} map
    so cryptic descriptors can read plainly. Returns {streams, total}."""
    labels = labels or {}
    groups: dict[str, list[dict]] = defaultdict(list)
    for t in txns:
        if t["cls"] == "income" and t["amount"] > 0:
            key = t.get("mkey") or merchant_key(t.get("desc", ""))
            if key:
                groups[key].append(t)
    streams = []
    for mkey, ts in groups.items():
        months = sorted({t["date"][:7] for t in ts if t.get("date")})
        amts = sorted(t["amount"] for t in ts)
        typical = amts[len(amts) // 2] if amts else 0.0
        if typical <= 0:
            continue
        streams.append({
            "key": mkey,
            "name": labels.get(mkey) or ts[-1]["desc"],
            "raw": ts[-1]["desc"],
            "monthly": round(typical, 2),
            "months": len(months),
        })
    streams.sort(key=lambda s: s["monthly"], reverse=True)
    return {"streams": streams, "total": round(sum(s["monthly"] for s in streams), 2)}


# ── Aggregation for the dashboard ──────────────────────────────────
def summarize(txns: list[dict], month: str | None = None) -> dict:
    """Compute the dashboard numbers. If `month` (YYYY-MM) is given,
    restrict the headline figures to it; trend always spans all months.

    Money Out excludes transfers (internal moves + card payments) so the
    total reflects real spending, not money shuffled between accounts."""
    months = sorted({t["date"][:7] for t in txns})
    scope = [t for t in txns if (month is None or t["date"][:7] == month)]

    income = sum(t["amount"] for t in scope if t["cls"] == "income")
    spend = -sum(t["amount"] for t in scope
                 if t["amount"] < 0 and t["cls"] not in ("transfer", "income"))
    fixed = -sum(t["amount"] for t in scope
                 if t["amount"] < 0 and t["cls"] == "fixed")
    variable = -sum(t["amount"] for t in scope
                    if t["amount"] < 0 and t["cls"] == "variable")
    debt_cost = -sum(t["amount"] for t in scope
                     if t["amount"] < 0 and t["cls"] == "debt")

    by_bucket: dict[str, float] = defaultdict(float)
    for t in scope:
        if t["amount"] < 0 and t["cls"] not in ("transfer", "income"):
            by_bucket[t["bucket"]] += -t["amount"]
    buckets = sorted(({"bucket": b, "cls": bucket_class(b),
                       "amount": round(a, 2)} for b, a in by_bucket.items()),
                     key=lambda r: r["amount"], reverse=True)

    trend = []
    for mo in months:
        mt = [t for t in txns if t["date"][:7] == mo]
        mi = sum(t["amount"] for t in mt if t["cls"] == "income")
        mo_out = -sum(t["amount"] for t in mt
                      if t["amount"] < 0 and t["cls"] not in ("transfer", "income"))
        trend.append({"month": mo, "in": round(mi, 2),
                      "out": round(mo_out, 2), "net": round(mi - mo_out, 2)})

    uncategorized = sum(1 for t in scope if t["cls"] == "review")
    return {
        "months": months,
        "month": month,
        "income": round(income, 2),
        "spend": round(spend, 2),
        "net": round(income - spend, 2),
        "fixed": round(fixed, 2),
        "variable": round(variable, 2),
        "debt_cost": round(debt_cost, 2),
        "buckets": buckets,
        "trend": trend,
        "uncategorized": uncategorized,
        "txn_count": len(scope),
    }


def extract_last_balance(headers, rows, mapping):
    """The running balance from the last dated row of a statement, so a
    HELOC/savings balance can be read straight from the import. Returns
    (balance, iso_date) or (None, None) if no balance column is mapped."""
    bi, di = mapping.get("balance"), mapping.get("date")
    if bi is None:
        return None, None
    best = None
    for r in rows:
        d = _norm_date(r[di]) if (di is not None and di < len(r)) else None
        v = _to_float(r[bi]) if bi < len(r) else None
        if d is None or v is None:
            continue
        if best is None or d >= best[1]:
            best = (v, d)
    return (round(best[0], 2), best[1]) if best else (None, None)


# ── Decision system: vital signs, modes, projections ───────────────
import math as _math

MODES = ("kill_debt", "cushion", "stop_overspend", "grow")
MODE_LABEL = {
    "kill_debt": "Pay off debt",
    "cushion": "Build a cushion",
    "stop_overspend": "Stop overspending",
    "grow": "Grow savings",
}


def _money(n):
    n = round(n or 0)
    return ("-$" if n < 0 else "$") + f"{abs(n):,}"


def _median(xs):
    xs = sorted(xs)
    if not xs:
        return 0.0
    n = len(xs)
    return xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2


# The bills she'd still owe if the paycheck stopped — what a real emergency
# fund has to cover. Deliberately excludes discretionary (dining, shopping,
# travel…) AND the one-time renovation buckets, so a lumpy reno month can't
# inflate the safety-net target.
ESSENTIAL_BUCKETS = {
    "Mortgage", "Rent", "HELOC", "Utilities", "Phone & Internet",
    "Insurance", "Groceries", "Health & Pharmacy", "Auto & Transport",
    "Gas & Fuel",
}


def _is_reno(t):
    """A one-time renovation outflow: tagged to a project or booked as a
    home-improvement purchase — kept out of the recurring-spend figures."""
    return bool(t.get("project_id")) or t.get("bucket") == "Home Improvement"


def monthly_figures(txns):
    """Median monthly income / spend / fixed / variable across the months
    present — robust to a partial first or last month. Also two normalized
    figures that ignore one-time renovation spending: `essential` (the bills
    an emergency fund must cover) and `spend_recurring` (everyday spend
    without the lumpy reno)."""
    months = sorted({t["date"][:7] for t in txns if t.get("date")})
    inc, spd, fix, var, ess, rec = [], [], [], [], [], []
    for mo in months:
        mt = [t for t in txns if t["date"][:7] == mo]
        inc.append(sum(t["amount"] for t in mt if t["cls"] == "income"))

        def out(cls=None, essential=False, no_reno=False):
            return -sum(
                t["amount"] for t in mt if t["amount"] < 0
                and t["cls"] not in ("transfer", "income")
                and (cls is None or t["cls"] == cls)
                and (not essential or t.get("bucket") in ESSENTIAL_BUCKETS)
                and (not no_reno or not _is_reno(t)))
        spd.append(out()); fix.append(out("fixed")); var.append(out("variable"))
        ess.append(out(essential=True, no_reno=True))
        rec.append(out(no_reno=True))
    return {"income": _median(inc), "spend": _median(spd),
            "fixed": _median(fix), "variable": _median(var),
            "essential": _median(ess), "spend_recurring": _median(rec),
            "n_months": len(months)}


def payoff_months(balance, apr, payment):
    """Months to clear `balance` at `payment`/mo and APR%. Returns
    (months, total_interest) — or (None, None) if the payment can't even
    cover the monthly interest (it would never pay off)."""
    balance = float(balance or 0); payment = float(payment or 0); apr = float(apr or 0)
    if balance <= 0:
        return (0, 0.0)
    if payment <= 0:
        return (None, None)
    i = apr / 100 / 12
    if i <= 0:
        n = _math.ceil(balance / payment)
        return (n, 0.0)
    if payment <= balance * i:
        return (None, None)
    n = _math.ceil(-_math.log(1 - balance * i / payment) / _math.log(1 + i))
    return (n, round(max(0.0, payment * n - balance), 2))


def debt_free_plan(debts, extra=0.0, cap_months=1200):
    """Simulate the debt AVALANCHE across all her debts: pay every minimum,
    throw any spare dollar at the highest-APR balance, and roll each cleared
    debt's payment onto the next. Returns the order she clears them, when,
    the interest each costs, and the debt-free month/total interest. Returns
    payoff=False if the payments can't outrun the interest."""
    ds = []
    for d in (debts or []):
        bal = float(d.get("balance") or 0)
        if bal <= 0:
            continue
        ds.append({"name": d.get("name") or "debt", "balance": bal,
                   "apr": float(d.get("apr") or 0),
                   "min": float(d.get("payment") or 0),
                   "interest": 0.0, "cleared_month": None})
    if not ds:
        return {"payoff": True, "months": 0, "total_interest": 0.0,
                "debts": [], "monthly_payment": 0.0, "extra": round(float(extra or 0), 2)}
    # highest APR first — the avalanche order
    ds.sort(key=lambda d: d["apr"], reverse=True)
    budget = round(sum(d["min"] for d in ds) + max(0.0, float(extra or 0)), 2)
    total_interest = 0.0
    month = 0
    while any(d["balance"] > 0.005 for d in ds) and month < cap_months:
        month += 1
        # accrue this month's interest
        for d in ds:
            if d["balance"] > 0:
                i = d["balance"] * d["apr"] / 1200.0
                d["balance"] += i
                d["interest"] += i
                total_interest += i
        # what she can pay this month, never more than what's owed
        remaining = min(budget, sum(d["balance"] for d in ds if d["balance"] > 0))
        if remaining <= 0.005:
            break
        # cover each minimum first (capped at the balance)…
        for d in ds:
            if d["balance"] > 0 and remaining > 0:
                pay = min(d["min"], d["balance"], remaining)
                d["balance"] -= pay
                remaining -= pay
        # …then avalanche the rest onto the highest-APR remaining balance
        for d in ds:                       # already APR-sorted
            if remaining <= 0.005:
                break
            if d["balance"] > 0:
                pay = min(d["balance"], remaining)
                d["balance"] -= pay
                remaining -= pay
        for d in ds:
            if d["balance"] <= 0.005 and d["cleared_month"] is None:
                d["balance"] = 0.0
                d["cleared_month"] = month
    payoff = all(d["balance"] <= 0.005 for d in ds)
    out_debts = [{"name": d["name"], "apr": round(d["apr"], 2),
                  "interest": round(d["interest"], 2),
                  "cleared_month": d["cleared_month"]}
                 for d in ds]
    return {
        "payoff": payoff,
        "months": month if payoff else None,
        "total_interest": round(total_interest, 2),
        "monthly_payment": budget,
        "extra": round(max(0.0, float(extra or 0)), 2),
        "debts": out_debts,
    }


def _light(value, green, yellow, higher_is_better=True):
    if value is None:
        return "gray"
    if higher_is_better:
        return "green" if value >= green else "yellow" if value >= yellow else "red"
    return "green" if value <= green else "yellow" if value <= yellow else "red"


LIQUID_KINDS = {"checking", "savings"}


def liquid_savings(accounts):
    """Cash on hand: the balances of her checking + savings accounts, read
    from the statements she imports. Returns (total, [(name, balance)…])."""
    liquid = [a for a in (accounts or [])
              if a.get("kind") in LIQUID_KINDS and a.get("balance") is not None]
    breakdown = [{"name": a.get("name"), "balance": round(float(a["balance"]), 2)}
                 for a in liquid]
    total = round(sum(b["balance"] for b in breakdown), 2)
    return total, breakdown


def vital_signs(txns, settings, accounts=None):
    """The four always-on diagnostics + the money figures behind them.
    settings carries what we can't derive: cushion_goal (months),
    heloc_balance, heloc_apr, heloc_payment, and an optional income override.
    Savings is read straight from her imported checking + savings balances
    (plus any 'savings_extra' held elsewhere); the manual `savings` setting
    is only a fallback for a book with no account balances yet."""
    s = settings or {}
    m = monthly_figures(txns)
    income_auto = m["income"]                       # median of recognized deposits
    manual_income = float(s.get("income") or 0)
    income = manual_income or income_auto
    income_source = "manual" if manual_income else ("deposits" if income_auto else "none")
    spend, fixed, variable = m["spend"], m["fixed"], m["variable"]
    # normalized figures that ignore lumpy one-time renovation spending
    essential = m["essential"]
    spend_recurring = m["spend_recurring"]
    # what the cushion is measured against: the bills a lost paycheck must
    # still cover. Fall back to recurring, then total, if we can't tell yet.
    cushion_base = essential or spend_recurring or spend
    from_accounts, liquid_breakdown = liquid_savings(accounts)
    savings_extra = float(s.get("savings_extra") or 0)
    if liquid_breakdown:
        savings = round(from_accounts + savings_extra, 2)
        savings_source = "accounts"
    else:
        savings = float(s.get("savings") or 0)
        savings_source = "manual"
    hb = float(s.get("heloc_balance") or 0)
    hapr = float(s.get("heloc_apr") or 0)
    hpay = float(s.get("heloc_payment") or 0)
    cb = float(s.get("card_balance") or 0)
    capr = float(s.get("card_apr") or 0)
    cpay = float(s.get("card_payment") or 0)
    goal = float(s.get("cushion_goal") or 4)

    months = sorted({t["date"][:7] for t in txns if t.get("date")})[-3:]
    net3 = []
    for mo in months:
        mt = [t for t in txns if t["date"][:7] == mo]
        mi = sum(t["amount"] for t in mt if t["cls"] == "income")
        # exclude one-time reno so a HELOC-funded remodel doesn't read as
        # "living beyond your means" — the renovation is tracked on its own tab
        mo_out = -sum(t["amount"] for t in mt if t["amount"] < 0
                      and t["cls"] not in ("transfer", "income") and not _is_reno(t))
        net3.append(mi - mo_out)
    avg_net = round(_median(net3), 2) if net3 else 0.0

    means_light = ("green" if avg_net >= 0
                   else "yellow" if income and avg_net > -0.10 * income else "red")
    ratio = (fixed / income * 100) if income else None
    months_saved = (savings / cushion_base) if (cushion_base and savings) else (0.0 if cushion_base else None)
    interest = hb * hapr / 100 / 12 if hb and hapr else 0.0
    pct_interest = round(interest / hpay * 100) if hpay else None
    heloc_light = ("gray" if not (hb and hpay)
                   else "green" if hpay > interest * 1.5
                   else "yellow" if hpay > interest else "red")

    lights = {
        "means": {"light": means_light, "value": avg_net,
                  "label": "Living within means", "unit": "$",
                  "note": ("about " + _money(avg_net) + "/mo " + ("to spare" if avg_net >= 0 else "short"))},
        "fixed": {"light": _light(ratio, 55, 70, higher_is_better=False),
                  "value": round(ratio, 1) if ratio is not None else None, "label": "Fixed bills vs. income", "unit": "%",
                  "note": (f"{round(ratio)}% of income is locked-in bills" if ratio is not None else "add your income")},
        "cushion": {"light": _light(months_saved if savings else None, 3, 1),
                    "value": round(months_saved, 1) if months_saved is not None else None, "label": "Cushion", "unit": "mo",
                    "note": (f"{_money(savings)} on hand — {round(months_saved,1)} of {round(goal)} months of bills" if (savings and months_saved is not None)
                             else f"{_money(savings)} on hand" if savings
                             else ("import an account to read savings" if savings_source == "accounts" else "set your savings balance"))},
        "heloc": {"light": heloc_light, "value": pct_interest, "label": "HELOC direction", "unit": "%",
                  "note": (f"{pct_interest}% of the payment is just interest" if pct_interest is not None else "set your HELOC balance")},
    }
    # Debt list for the pay-off-debt mode — highest APR first (avalanche).
    debts = []
    if hb > 0:
        debts.append({"name": "HELOC", "balance": hb, "apr": hapr, "payment": hpay})
    if cb > 0:
        debts.append({"name": "credit card", "balance": cb, "apr": capr, "payment": cpay})
    debts.sort(key=lambda d: d["apr"], reverse=True)
    return {
        "lights": lights, "income": round(income, 2), "spend": round(spend, 2),
        "income_auto": round(income_auto, 2), "income_source": income_source,
        "fixed": round(fixed, 2), "variable": round(variable, 2), "avg_net": avg_net,
        "essential": round(essential, 2), "spend_recurring": round(spend_recurring, 2),
        "cushion_base": round(cushion_base, 2),
        "savings": savings, "cushion_goal": goal,
        "savings_source": savings_source, "savings_from_accounts": from_accounts,
        "savings_extra": savings_extra, "liquid_accounts": liquid_breakdown,
        "heloc_balance": hb, "heloc_apr": hapr, "heloc_payment": hpay,
        "card_balance": cb, "card_apr": capr, "card_payment": cpay,
        "debts": debts, "after_bills": round(income - fixed, 2),
    }


def recommendation(vs, mode):
    """Given the vital signs and the chosen focus, return the single
    headline + one move + a plain-language projection for that mode."""
    mode = mode if mode in MODES else "kill_debt"
    surplus = vs["avg_net"]
    spend = vs["spend"]

    if mode == "stop_overspend":
        head = {"k": "Left after bills", "v": vs["after_bills"]}
        move = (f"You're living within your means — about {_money(surplus)} to spare each month."
                if surplus >= 0 else
                f"You're spending about {_money(-surplus)} more than you make. Ease off your biggest everyday bucket.")
        return {"headline": head, "move": move, "projection": None}

    if mode == "cushion":
        goal_amt = vs["cushion_goal"] * spend
        need = max(0.0, goal_amt - vs["savings"])
        head = {"k": "Cushion", "v_txt": f"{vs['lights']['cushion']['value'] or 0} of {round(vs['cushion_goal'])} mo"}
        if surplus > 0 and need > 0:
            n = _math.ceil(need / surplus)
            move = f"Move {_money(surplus)} to savings this month."
            proj = f"At {_money(surplus)}/mo you reach a {round(vs['cushion_goal'])}-month cushion in about {n} months."
        elif need <= 0:
            move = "Your cushion goal is met — nice. Switch focus to the HELOC."
            proj = None
        else:
            move = "No surplus to save this month — first close the gap in spending."
            proj = None
        return {"headline": head, "move": move, "projection": proj}

    if mode == "grow":
        head = {"k": "Monthly surplus", "v": surplus}
        if surplus > 0 and vs["lights"]["cushion"]["light"] in ("green", "gray"):
            move = f"You're steady — set {_money(surplus)}/mo to auto-invest for the long term."
            proj = f"{_money(surplus)}/mo is about {_money(surplus*12)}/year toward retirement."
        elif surplus > 0:
            move = f"Build the cushion to 3 months first, then send {_money(surplus)}/mo to investing."
            proj = None
        else:
            move = "No surplus yet — steady the month before investing."
            proj = None
        return {"headline": head, "move": move, "projection": proj}

    # kill_debt (default) — avalanche: attack the HIGHEST-rate debt first.
    debts = vs.get("debts") or []
    total_debt = sum(d["balance"] for d in debts)
    if not debts:
        return {"headline": {"k": "Debt", "v": 0},
                "move": "Set your HELOC and/or card balance, rate and payment to see the payoff plan.",
                "projection": None}
    target = debts[0]                                   # highest APR
    head = {"k": f"{target['name'].title()} balance", "v": target["balance"]}
    apr_txt = f"{round(target['apr'], 2)}%"
    others = f" (you carry {_money(total_debt)} of debt across {len(debts)})" if len(debts) > 1 else ""
    n, total_int = payoff_months(target["balance"], target["apr"], target["payment"])
    lead = (f"Put every extra dollar on the {target['name']} first — it's your most expensive debt at {apr_txt}{others}. "
            if len(debts) > 1 else "")
    if n is None:
        move = lead + "That payment barely covers its interest, so even a small increase starts shrinking the balance."
        proj = None
    else:
        extra = max(0.0, surplus)
        n2, int2 = payoff_months(target["balance"], target["apr"], target["payment"] + extra)
        if extra > 0 and n2 and n2 < n:
            move = (lead + f"Send your {_money(extra)} surplus to it — clear in {n2} months instead of {n}, "
                    f"saving {_money((total_int or 0) - (int2 or 0))} in interest.")
        else:
            move = lead + f"On track to clear it in about {n} months at the current payment."
        nxt = (" Then roll that whole payment onto the next debt." if len(debts) > 1 else "")
        proj = f"At the current payment: {target['name']} clears in ~{n} months, ~{_money(total_int)} interest.{nxt}"
    return {"headline": head, "move": move, "projection": proj}


def this_month(txns, settings, mode="kill_debt", accounts=None):
    """Bundle the vital signs + the mode's recommendation for the tab."""
    vs = vital_signs(txns, settings, accounts)
    debts = vs.get("debts") or []
    surplus = max(0.0, vs.get("avg_net") or 0)
    debt_plan = None
    if debts:
        debt_plan = {
            "surplus": round(surplus, 2),
            "minimums": debt_free_plan(debts, extra=0),
            "with_surplus": debt_free_plan(debts, extra=surplus),
        }
    return {
        "vitals": vs,
        "mode": mode if mode in MODES else "kill_debt",
        "modes": [{"key": k, "label": MODE_LABEL[k]} for k in MODES],
        "rec": recommendation(vs, mode),
        "debt_plan": debt_plan,
        "income_streams": income_streams(txns, (settings or {}).get("income_labels")),
    }


# ── Renovation / project reconciliation ────────────────────────────
RENO_VENDORS = [
    "home depot", "homedepot", "lowe", "floor & decor", "floor and decor",
    "ferguson", "ace hardware", "menards", "the tile", "tile shop", "build.com",
    "sherwin", "benjamin moore", "flooring", "hardwood floor", "carpet",
    "cabinet", "countertop", "granite", "quartz", "appliance", "permit",
    "contractor", "construction", "roofing", "hvac", "plumbing supply",
    "electric supply", "lumber", "glass & mirror", "hardware store", "restoration",
    "remodel", "kitchen & bath", "window", "paint ", "glass", "andersen",
    "pella", "milgard", "marvin", "door", "cabinetry", "granite", "masonry",
]


# Person-to-person payment apps — a "ZELLE ... TO ROSALES" is money to a
# person (here, renovation labor), never an internal account move.
_P2P_RE = re.compile(r"\b(zelle|venmo|cash\s*app|cashapp)\b", re.I)
_PAYEE_STOP = {"the", "llc", "inc", "payment", "transfer", "money", "online",
               "web", "mobile", "and", "for"}


def is_p2p(desc: str) -> bool:
    return bool(_P2P_RE.search(desc or ""))


def payee_fragment(desc: str) -> str:
    """A stable, lowercase key for the person/vendor paid, good for matching
    the same payee on future statements. For a person-to-person app it's the
    name after the last 'to ' ("...TO ROSALES,FE" -> "rosales"); otherwise
    the merchant key."""
    low = (desc or "").lower()
    if is_p2p(low):
        idx = low.rfind(" to ")
        tail = low[idx + 4:] if idx >= 0 else low
        toks = [t for t in re.findall(r"[a-z]{3,}", tail) if t not in _PAYEE_STOP]
        if toks:
            return max(toks, key=len)          # surname is the longest, stable bit
    return merchant_key(desc)


def payee_matches(desc, payee):
    """Whole-word, case-insensitive test for a learned payee inside a
    description — so a short surname like "lee" matches "…TO LEE" but NOT
    "SLEEP NUMBER". Payees under 3 chars are ignored as too broad."""
    p = (payee or "").strip().lower()
    if len(p) < 3:
        return False
    return re.search(r"\b" + re.escape(p) + r"\b", (desc or "").lower()) is not None


def _in_window(t, start, end):
    if t.get("project_id") or t["amount"] >= 0:
        return False
    d = t.get("date", "") or ""
    return not ((start and d < start) or (end and d > end))


def suggest_reno(txns, start=None, end=None, payees=None):
    """Untagged outflows within the project window that look like renovation
    spend — a known vendor (Home Depot, contractor, glass) OR a learned
    labor payee (a person she's marked as renovation labor). Candidates for
    one-tap or automatic tagging."""
    pay = [p.lower() for p in (payees or []) if p]
    out = []
    for t in txns:
        if not _in_window(t, start, end):
            continue
        low = t["desc"].lower()
        if any(k in low for k in RENO_VENDORS) or any(payee_matches(low, p) for p in pay):
            out.append(t)
    return out


def labor_candidates(txns, start=None, end=None, payees=None):
    """Untagged person-to-person payments (Zelle/Venmo) within the window
    that AREN'T yet a known vendor or learned payee — likely renovation
    labor worth one-tap confirming ('tag + remember this payee')."""
    pay = [p.lower() for p in (payees or []) if p]
    out = []
    for t in txns:
        if not _in_window(t, start, end):
            continue
        low = t["desc"].lower()
        if not is_p2p(low):
            continue
        if any(payee_matches(low, p) for p in pay) or any(k in low for k in RENO_VENDORS):
            continue
        row = dict(t)
        row["payee"] = payee_fragment(t["desc"])
        out.append(row)
    return out


def project_summary(tagged, settings, interest_paid=0.0):
    """Reconcile a project's tagged spend against the HELOC. Returns the
    ledger totals, vendor + monthly breakdown, and the true-cost / payoff
    figures the tab shows. `interest_paid` = HELOC interest booked so far
    (summed by the caller from the ledger); heloc_* come from settings."""
    s = settings or {}
    spend = round(-sum(t["amount"] for t in tagged if t["amount"] < 0), 2)

    by = defaultdict(float)
    for t in tagged:
        if t["amount"] < 0:
            by[t["desc"]] += -t["amount"]
    vendors = sorted(({"name": k, "amount": round(v, 2)} for k, v in by.items()),
                     key=lambda r: r["amount"], reverse=True)

    bym = defaultdict(float)
    for t in tagged:
        if t["amount"] < 0 and t.get("date"):
            bym[t["date"][:7]] += -t["amount"]
    by_month = [{"month": k, "amount": round(bym[k], 2)} for k in sorted(bym)]

    hb = float(s.get("heloc_balance") or 0)
    hapr = float(s.get("heloc_apr") or 0)
    hpay = float(s.get("heloc_payment") or 0)
    budget = float(s.get("reno_budget") or 0)
    n, total_int = payoff_months(hb, hapr, hpay)
    monthly_carry = round(hb * hapr / 100 / 12, 2) if (hb and hapr) else 0.0
    true_cost = round(spend + (interest_paid or 0), 2)

    return {
        "spend": spend, "vendors": vendors, "by_month": by_month,
        "count": sum(1 for t in tagged if t["amount"] < 0),
        "budget": budget, "over_under": round(spend - budget, 2) if budget else None,
        "heloc_balance": hb, "heloc_apr": hapr, "heloc_payment": hpay,
        "monthly_carry": monthly_carry, "interest_paid": round(interest_paid or 0, 2),
        "true_cost": true_cost,
        "payoff_months": n, "payoff_interest": total_int,
    }


# ── Kitchen budget builder ─────────────────────────────────────────
BUDGET_SECTIONS = [
    "Cabinets", "Countertops", "Appliances", "Flooring", "Backsplash",
    "Fixtures & Lighting", "Plumbing", "Electrical", "Demo & Prep",
    "Paint & Walls", "Permits & Fees", "Labor & Install", "Other",
]


def kitchen_quantities(m):
    """Derive material quantities from the kitchen measurements she enters
    (falls back to a direct sqft/lin-ft field, then a typical value)."""
    m = m or {}
    fl, fw = float(m.get("floor_len_ft") or 0), float(m.get("floor_wid_ft") or 0)
    counter_lf = float(m.get("counter_run_ft") or 0)
    depth_in = float(m.get("counter_depth_in") or 25.5)
    cab_lf = float(m.get("cabinet_lf") or 0) or counter_lf
    bs_len = float(m.get("backsplash_len_ft") or 0) or counter_lf
    bs_h_in = float(m.get("backsplash_height_in") or 18)
    return {
        "floor_sqft": round(fl * fw, 1) if (fl and fw) else float(m.get("floor_sqft") or 0),
        "counter_sqft": round(counter_lf * (depth_in / 12), 1) if counter_lf else float(m.get("counter_sqft") or 0),
        "cabinet_lf": cab_lf,
        "backsplash_sqft": round(bs_len * (bs_h_in / 12), 1) if bs_len else float(m.get("backsplash_sqft") or 0),
    }


def kitchen_seed_template(meta=None):
    """A realistic 'nice full kitchen reno' starting budget for San Leandro
    / East Bay, priced off 2026 web research (semi-custom cabinets ~$525/lf,
    quartz ~$95/sqft installed, LVP ~$8/sqft, backsplash ~$30/sqft, high
    appliance package, Alameda permits, Bay-Area labor). Quantities scale
    to her measurements when present. All fully editable afterward."""
    q = kitchen_quantities(meta)
    cab = q["cabinet_lf"] or 25
    counter = q["counter_sqft"] or 55
    floor = q["floor_sqft"] or 200
    bs = q["backsplash_sqft"] or 35
    return [
        # Cabinet finish — pick one (poplar/paint is cheapest, walnut priciest).
        {"section": "Cabinets", "name": "Cabinets — semi-custom, painted", "qty": cab, "unit": "lin ft", "unit_cost": 525, "labor": 0, "opt_group": "Cabinet finish", "chosen": True},
        {"section": "Cabinets", "name": "Cabinets — white oak (custom)", "qty": cab, "unit": "lin ft", "unit_cost": 850, "labor": 0, "opt_group": "Cabinet finish", "chosen": False},
        {"section": "Cabinets", "name": "Cabinets — walnut (custom)", "qty": cab, "unit": "lin ft", "unit_cost": 1100, "labor": 0, "opt_group": "Cabinet finish", "chosen": False},
        # Countertop material — pick one.
        {"section": "Countertops", "name": "Quartz (engineered) — no maintenance", "qty": counter, "unit": "sq ft", "unit_cost": 95, "labor": 0, "opt_group": "Countertop material", "chosen": True},
        {"section": "Countertops", "name": "Quartzite (natural) — seal occasionally", "qty": counter, "unit": "sq ft", "unit_cost": 120, "labor": 0, "opt_group": "Countertop material", "chosen": False},
        {"section": "Countertops", "name": "Marble — soft, patinas/etches", "qty": counter, "unit": "sq ft", "unit_cost": 115, "labor": 0, "opt_group": "Countertop material", "chosen": False},
        {"section": "Countertops", "name": "Soapstone — non-porous, patinas", "qty": counter, "unit": "sq ft", "unit_cost": 110, "labor": 0, "opt_group": "Countertop material", "chosen": False},
        {"section": "Appliances", "name": "Range (gas, 30 in)", "qty": 1, "unit": "ea", "unit_cost": 2800, "labor": 0},
        {"section": "Appliances", "name": "Refrigerator (counter-depth)", "qty": 1, "unit": "ea", "unit_cost": 2800, "labor": 0},
        {"section": "Appliances", "name": "Dishwasher", "qty": 1, "unit": "ea", "unit_cost": 1200, "labor": 0},
        {"section": "Appliances", "name": "Range hood", "qty": 1, "unit": "ea", "unit_cost": 1000, "labor": 0},
        {"section": "Appliances", "name": "Microwave (built-in)", "qty": 1, "unit": "ea", "unit_cost": 600, "labor": 0},
        {"section": "Flooring", "name": "Luxury vinyl plank (installed)", "qty": floor, "unit": "sq ft", "unit_cost": 8, "labor": 0},
        {"section": "Backsplash", "name": "Tile backsplash (installed)", "qty": bs, "unit": "sq ft", "unit_cost": 30, "labor": 0},
        {"section": "Fixtures & Lighting", "name": "Sink (undermount)", "qty": 1, "unit": "ea", "unit_cost": 450, "labor": 0},
        {"section": "Fixtures & Lighting", "name": "Faucet", "qty": 1, "unit": "ea", "unit_cost": 400, "labor": 0},
        {"section": "Fixtures & Lighting", "name": "Lighting (recessed + pendants + under-cabinet)", "qty": 1, "unit": "set", "unit_cost": 1600, "labor": 0},
        {"section": "Plumbing", "name": "Plumbing (rough-in + connect)", "qty": 1, "unit": "job", "unit_cost": 0, "labor": 2800},
        {"section": "Electrical", "name": "Electrical (circuits + lighting)", "qty": 1, "unit": "job", "unit_cost": 0, "labor": 2800},
        {"section": "Demo & Prep", "name": "Demolition & disposal", "qty": 1, "unit": "job", "unit_cost": 0, "labor": 2200},
        {"section": "Paint & Walls", "name": "Drywall repair & paint", "qty": 1, "unit": "job", "unit_cost": 0, "labor": 2200},
        {"section": "Permits & Fees", "name": "Permits (San Leandro / Alameda Co.)", "qty": 1, "unit": "ea", "unit_cost": 1400, "labor": 0},
        {"section": "Labor & Install", "name": "General labor & project management", "qty": 1, "unit": "job", "unit_cost": 0, "labor": 22000},
    ]


def _item_total(it):
    return round(float(it.get("qty", 1)) * float(it.get("unit_cost", 0)) + float(it.get("labor", 0)), 2)


def budget_summary(items, meta=None):
    """Roll the line items up by section, apply a contingency %, compare to
    her target, and compute HELOC headroom against the home's equity."""
    m = meta or {}
    cont_pct = float(m.get("contingency_pct") if m.get("contingency_pct") is not None else 15)
    by_section = {}
    owned_total = 0.0
    total_planned = 0.0          # sum of budgeted allowances (counting items)
    freed = 0.0                  # money saved on items now under their plan
    over = 0.0                   # extra needed on items now over their plan
    has_plan = False
    for it in items:
        by_section.setdefault(it["section"], 0.0)
        # An unchosen alternative in an option group doesn't count.
        if it.get("opt_group") and not it.get("chosen", True):
            continue
        # "Already have it" items stay in the plan but cost $0 toward it.
        if it.get("owned"):
            owned_total += _item_total(it)
            continue
        cur = _item_total(it)
        by_section[it["section"]] += cur
        # Reallocation: compare the live estimate to the budgeted plan.
        plan = it.get("planned")
        if plan is not None:
            has_plan = True
            plan = float(plan)
            total_planned += plan
            if cur < plan:
                freed += plan - cur
            elif cur > plan:
                over += cur - plan
        else:
            total_planned += cur     # no baseline yet → on plan by definition
    subtotal = round(sum(by_section.values()), 2)
    contingency = round(subtotal * cont_pct / 100, 2)
    total = round(subtotal + contingency, 2)
    sections = [{"section": s, "amount": round(a, 2)}
                for s, a in sorted(by_section.items(), key=lambda x: -x[1])]
    target = float(m.get("budget_target") or 0)

    home = float(m.get("home_value") or 0)
    mortgage = float(m.get("mortgage_balance") or 0)
    cltv = float(m.get("target_cltv") or 80)
    hlimit = float(m.get("heloc_limit") or 0)
    hbal = float(m.get("heloc_balance") or 0)
    max_lien = home * cltv / 100 if home else 0
    potential_limit = max(0.0, max_lien - mortgage) if home else 0.0
    potential_increase = (max(0.0, potential_limit - hlimit) if hlimit else potential_limit)
    available_now = max(0.0, hlimit - hbal) if hlimit else 0.0
    # how much of THIS budget the HELOC could cover if raised
    can_fund = min(total, available_now + potential_increase) if (available_now or potential_increase) else 0.0

    return {
        "sections": sections, "subtotal": subtotal,
        "contingency_pct": cont_pct, "contingency": contingency, "total": total,
        "owned_total": round(owned_total, 2),
        "target": round(target, 2),
        "over_under": round(total - target, 2) if target else None,
        "reallocation": {
            "has_plan": has_plan,
            "planned": round(total_planned, 2),
            "current": subtotal,
            "freed": round(freed, 2),
            "over": round(over, 2),
            "net_vs_plan": round(subtotal - total_planned, 2),
        },
        "financing": {
            "home_value": home, "mortgage_balance": mortgage, "target_cltv": cltv,
            "heloc_limit": hlimit, "heloc_balance": hbal,
            "available_now": round(available_now, 2),
            "potential_limit": round(potential_limit, 2),
            "potential_increase": round(potential_increase, 2),
            "can_fund": round(can_fund, 2),
        },
    }


def optimize_budget(items, meta=None):
    """Auto-pick the option in each compare-group that spends the MOST of her
    budget target WITHOUT going over — the nicest kitchen that still fits.
    Only option groups move; fixed lines and 'owned' items are left alone.
    Returns {feasible, target, projected, picks: {opt_group: item_id}}."""
    m = meta or {}
    target = float(m.get("budget_target") or 0)
    cont = float(m.get("contingency_pct") if m.get("contingency_pct") is not None else 15)
    if target <= 0:
        return {"feasible": False, "reason": "no_target", "picks": {}}

    groups, fixed = {}, 0.0
    for it in items:
        if it.get("owned"):
            continue
        g = it.get("opt_group")
        if g:
            groups.setdefault(g, []).append(it)
        else:
            fixed += _item_total(it)

    factor = 1 + cont / 100.0
    budget_for_opts = target / factor - fixed        # what the option groups may sum to

    if not groups:
        projected = round(fixed * factor, 2)
        return {"feasible": projected <= target, "target": round(target, 2),
                "projected": projected, "picks": {}, "reason": "no_options"}

    order = sorted(groups)
    cheapest_sum = sum(min(_item_total(o) for o in groups[g]) for g in order)
    picks = {}

    if cheapest_sum > budget_for_opts:
        # can't fit even the cheapest of each — pick cheapest everywhere (best effort)
        for g in order:
            picks[g] = min(groups[g], key=_item_total)["id"]
        chosen_sum, feasible = cheapest_sum, False
    else:
        # brute force is fine for a handful of small groups; guard the blow-up
        combos = 1
        for g in order:
            combos *= len(groups[g])
        if combos <= 100000:
            best, best_sum = None, -1.0
            for combo in _itertools_product([groups[g] for g in order]):
                s = sum(_item_total(o) for o in combo)
                if s <= budget_for_opts + 0.01 and s > best_sum:
                    best_sum, best = s, combo
            for g, o in zip(order, best):
                picks[g] = o["id"]
            chosen_sum = best_sum
        else:
            # greedy fallback: start cheapest, upgrade the biggest affordable step
            chosen = {g: min(groups[g], key=_item_total) for g in order}
            chosen_sum = sum(_item_total(chosen[g]) for g in order)
            improved = True
            while improved:
                improved = False
                for g in order:
                    room = budget_for_opts - chosen_sum
                    cur = _item_total(chosen[g])
                    ups = [o for o in groups[g] if 0 < _item_total(o) - cur <= room]
                    if ups:
                        nxt = max(ups, key=_item_total)
                        chosen_sum += _item_total(nxt) - cur
                        chosen[g] = nxt
                        improved = True
            for g in order:
                picks[g] = chosen[g]["id"]
        feasible = True

    projected = round((fixed + chosen_sum) * factor, 2)
    return {"feasible": feasible, "target": round(target, 2),
            "projected": projected, "picks": picks,
            "budget_for_options": round(budget_for_opts, 2)}


def _itertools_product(pools):
    import itertools
    return itertools.product(*pools)


# ── Retirement ("when can she retire?") ─────────────────────────────
# Frances's starting figures, lifted from her long-running retirement
# spreadsheet (CalPERS "2% @ 55" service pension near the 2.5% cap, plus
# Social Security by claim age, plus her low-rate LoanDepot mortgage).
# These SEED her book; everything is editable in the UI afterward. The
# HELOC and credit-card payments come from the same statements the rest
# of the tool already tracks — retirement just reuses them as fixed bills.
RETIRE_SEED = {
    "birth_year": 1960,
    # realistic monthly living cost in retirement (her "basis for the cal")
    "retire_expenses": 7000,
    # LoanDepot first mortgage — 2.99%, cheap, so the payoff engine leaves
    # it alone; it's simply a bill until it clears. Payment is the FULL
    # monthly ACH from her statement ($2,664 PITI = ~$1,836 P&I + escrow
    # for property tax & insurance), which is her real cash outflow.
    "mortgage_balance": 377000,
    "mortgage_payment": 2664,
    "mortgage_rate": 2.99,
    # CalPERS monthly pension by the year she stops working (col J of her
    # "retire" tab). Each extra year is worth ~$550–670/mo, for life.
    "pension_by_year": {
        "2026": 8017, "2027": 8685, "2028": 9216, "2029": 9769, "2030": 10344,
    },
    # Social Security monthly benefit by the age she first claims.
    "ss_by_age": {
        "62": 1890, "63": 2248, "64": 2502, "65": 2808, "66": 3057,
        "67": 3405, "68": 3505, "69": 3815, "70": 4338,
    },
    # her current picks (full-retirement-age SS is 67 for a 1960 birth year)
    "retire_year": 2027,
    "ss_claim_age": 67,
}


def retirement_seed():
    import copy
    return copy.deepcopy(RETIRE_SEED)


def _int_keyed(d):
    out = {}
    for k, v in (d or {}).items():
        try:
            out[int(k)] = float(v)
        except (TypeError, ValueError):
            continue
    return out


def retirement_plan(settings):
    """Weave her CalPERS pension + Social Security + the debts the tool
    already tracks into one 'when can she retire, and is she covered?'
    picture. Reads settings['retirement']; heloc_/card_ come from the top
    level of settings (same place This Month reads them)."""
    s = settings or {}
    ret = s.get("retirement") or {}
    if not ret.get("pension_by_year"):
        return {"configured": False}

    birth = int(ret.get("birth_year") or 0)
    expenses = float(ret.get("retire_expenses") or 0)
    pension_by_year = _int_keyed(ret.get("pension_by_year"))
    ss_by_age = _int_keyed(ret.get("ss_by_age"))
    mort_bal = float(ret.get("mortgage_balance") or 0)
    mort_pay = float(ret.get("mortgage_payment") or 0)
    mort_rate = float(ret.get("mortgage_rate") or 0)

    # Debt service she'd still carry into retirement, from her statements.
    heloc_pay = float(s.get("heloc_payment") or 0)
    card_pay = float(s.get("card_payment") or 0)
    debt_bills = [
        {"name": "Mortgage (LoanDepot)", "payment": round(mort_pay, 2), "rate": mort_rate,
         "note": "2.99% — cheap, kept as a bill until it clears"},
        {"name": "HELOC (Golden 1)", "payment": round(heloc_pay, 2),
         "rate": float(s.get("heloc_apr") or 0), "note": "renovation debt"},
        {"name": "Credit card (Golden 1)", "payment": round(card_pay, 2),
         "rate": float(s.get("card_apr") or 0), "note": "highest rate — clear first"},
    ]
    debt_bills = [d for d in debt_bills if d["payment"] > 0]
    debt_total = round(sum(d["payment"] for d in debt_bills), 2)
    need = round(expenses + debt_total, 2)

    claim_age = int(ret.get("ss_claim_age") or 67)
    ss_monthly = ss_by_age.get(claim_age, 0.0)

    def age_in(year):
        return (year - birth) if birth else None

    def scenario(year):
        pension = pension_by_year.get(year, 0.0)
        claim_year = (birth + claim_age) if birth else year
        # Before SS starts she lives on pension alone (the "bridge" years).
        bridge = max(0, claim_year - year)
        pension_only = round(pension - need, 2)
        with_ss = round(pension + ss_monthly - need, 2)
        return {
            "year": year, "age": age_in(year),
            "pension": round(pension, 2),
            "ss_monthly": round(ss_monthly, 2),
            "ss_starts_year": claim_year,
            "bridge_years": bridge,
            "income_before_ss": round(pension, 2),
            "income_with_ss": round(pension + ss_monthly, 2),
            "surplus_before_ss": pension_only,
            "surplus_with_ss": with_ss,
            "covered": with_ss >= 0,
            "covered_before_ss": pension_only >= 0,
        }

    years = sorted(pension_by_year)
    scenarios = [scenario(y) for y in years]
    chosen_year = int(ret.get("retire_year") or (years[0] if years else 0))
    chosen = next((sc for sc in scenarios if sc["year"] == chosen_year),
                  scenarios[0] if scenarios else None)

    # Earliest year she's fully covered once SS is on — the "green light".
    earliest_ok = next((sc["year"] for sc in scenarios if sc["covered"]), None)

    # Cost of retiring one year earlier than chosen / gain of waiting one.
    def delta(from_y, to_y):
        a = pension_by_year.get(from_y)
        b = pension_by_year.get(to_y)
        return round(b - a, 2) if (a is not None and b is not None) else None

    wait_gain = delta(chosen_year, chosen_year + 1) if chosen else None
    early_cost = delta(chosen_year - 1, chosen_year) if chosen else None

    return {
        "configured": True,
        "birth_year": birth,
        "expenses": round(expenses, 2),
        "debt_bills": debt_bills,
        "debt_total": debt_total,
        "need": need,
        "claim_age": claim_age,
        "ss_monthly": round(ss_monthly, 2),
        "ss_by_age": {str(k): round(v, 2) for k, v in sorted(ss_by_age.items())},
        "claim_ages": sorted(ss_by_age),
        "mortgage_balance": round(mort_bal, 2),
        "chosen": chosen,
        "scenarios": scenarios,
        "earliest_covered_year": earliest_ok,
        "wait_one_year_gain": wait_gain,
        "retire_early_cost": early_cost,
    }


# ── Net worth (the whole picture in one number) ────────────────────
DEFAULT_HOME_VALUE = 950000.0     # she told us; editable


def net_worth(settings, accounts=None):
    """Assets minus liabilities. Assets = the home, cash on hand (her
    imported checking + savings), and any investment / other assets she
    enters by hand (Fidelity, 401k, a second property…). Liabilities = the
    mortgage, HELOC and card the tool already tracks from her statements."""
    s = settings or {}
    ret = s.get("retirement") or {}

    home = float(s.get("home_value") or ret.get("home_value") or DEFAULT_HOME_VALUE)
    cash, cash_break = liquid_savings(accounts)
    manual = []
    for a in (s.get("assets") or []):
        try:
            manual.append({"id": a.get("id"), "name": str(a.get("name") or "Asset"),
                           "value": round(float(a.get("value") or 0), 2),
                           "kind": a.get("kind") or "investment"})
        except (TypeError, ValueError):
            continue

    assets = [{"name": "Home", "value": round(home, 2), "kind": "property", "auto": True}]
    if cash or cash_break:
        assets.append({"name": "Cash on hand", "value": cash, "kind": "cash", "auto": True,
                       "detail": " + ".join(f"{b['name']} {_money(b['balance'])}" for b in cash_break)})
    assets += manual
    total_assets = round(sum(a["value"] for a in assets), 2)

    mortgage = float(ret.get("mortgage_balance") or 0)
    heloc = float(s.get("heloc_balance") or 0)
    card = float(s.get("card_balance") or 0)
    liabilities = [
        {"name": "Mortgage (LoanDepot)", "value": round(mortgage, 2)},
        {"name": "HELOC (Golden 1)", "value": round(heloc, 2)},
        {"name": "Credit card (Golden 1)", "value": round(card, 2)},
    ]
    liabilities = [l for l in liabilities if l["value"] > 0]
    total_liab = round(mortgage + heloc + card, 2)

    invest_total = round(sum(a["value"] for a in manual if a["kind"] == "investment"), 2)
    home_equity = round(home - mortgage - heloc, 2)   # equity left in the house

    return {
        "assets": assets, "liabilities": liabilities,
        "total_assets": total_assets, "total_liabilities": total_liab,
        "net_worth": round(total_assets - total_liab, 2),
        "home_value": round(home, 2), "home_equity": home_equity,
        "cash": cash, "investments": invest_total,
        "manual_assets": manual,
    }


# ── Rental scenario ("what if she rented the house out?") ──────────
def rental_scenario(settings, params=None):
    """A plain-language cash-flow model for renting the house: rent minus
    the mortgage (PITI), the HELOC, and operating costs (management,
    vacancy, maintenance). Sensible Bay-Area defaults; every input editable
    via settings['rental_*'] or the params override."""
    s = settings or {}
    p = params or {}
    ret = s.get("retirement") or {}

    def num(key, default):
        for src in (p, s):
            v = src.get(key)
            if v is not None:
                try:
                    return float(v)
                except (TypeError, ValueError):
                    pass
        return float(default)

    home = float(s.get("home_value") or ret.get("home_value") or DEFAULT_HOME_VALUE)
    rent = num("rental_rent", 5000)
    mortgage = float(ret.get("mortgage_payment") or 0)
    heloc = float(s.get("heloc_payment") or 0)
    mgmt_pct = num("rental_mgmt_pct", 8)          # property manager
    vacancy_pct = num("rental_vacancy_pct", 5)    # empty-months allowance
    maint_pct = num("rental_maint_pct", 1)        # annual % of home value

    mgmt = round(rent * mgmt_pct / 100, 2)
    vacancy = round(rent * vacancy_pct / 100, 2)
    maintenance = round(home * maint_pct / 100 / 12, 2)
    operating = round(mgmt + vacancy + maintenance, 2)
    debt_service = round(mortgage + heloc, 2)
    total_costs = round(debt_service + operating, 2)
    net = round(rent - total_costs, 2)

    return {
        "home_value": round(home, 2), "rent": round(rent, 2),
        "mortgage": round(mortgage, 2), "heloc": round(heloc, 2),
        "mgmt": mgmt, "vacancy": vacancy, "maintenance": maintenance,
        "operating": operating, "debt_service": debt_service,
        "total_costs": total_costs, "net_monthly": net,
        "net_annual": round(net * 12, 2),
        "gross_yield_pct": round(rent * 12 / home * 100, 2) if home else None,
        "mgmt_pct": mgmt_pct, "vacancy_pct": vacancy_pct, "maint_pct": maint_pct,
        "covers_costs": net >= 0,
    }


# ── The Roadmap (order of operations she can follow) ────────────────
# One ordered path over everything the tool tracks. Each milestone reads
# her live numbers and reports done / now / later, so a non-technical
# person always sees the single next move instead of four dashboards.
def _progress_pct(have, target):
    if not target or target <= 0:
        return None
    return max(0, min(100, round(have / target * 100)))


def money_roadmap(vitals, reno=None, retire=None):
    """Build the ordered milestone list and mark which one she's on. The
    order is by what protects each dollar most: a covered month, a small
    buffer, then the priciest debt, a real safety net, the kitchen goal,
    the cheaper HELOC, and finally the retirement date. `reno` and `retire`
    are small summary dicts the caller assembles from the budget and the
    retirement plan; either may be None when there's nothing there yet."""
    v = vitals or {}
    reno = reno or {}
    retire = retire or {}

    spend = float(v.get("spend") or 0)
    # the safety net is sized to essential bills (mortgage, utilities,
    # insurance, groceries…), NOT total spend — so a one-time renovation
    # month doesn't balloon the target. Falls back to spend if we can't
    # separate essentials yet.
    essential = float(v.get("essential") or 0) or float(v.get("spend_recurring") or 0) or spend
    savings = float(v.get("savings") or 0)
    avg_net = float(v.get("avg_net") or 0)
    card = float(v.get("card_balance") or 0)
    card_apr = float(v.get("card_apr") or 0)
    heloc = float(v.get("heloc_balance") or 0)
    heloc_apr = float(v.get("heloc_apr") or 0)

    starter_target = 2000.0
    safety_target = round(3 * essential, 2) if essential else 0.0

    steps = []

    def add(key, title, why, done, metric, action, tab, mode=None,
            progress=None, parallel=False):
        steps.append({
            "key": key, "title": title, "why": why, "done": bool(done),
            "metric": metric, "action": action, "tab": tab, "mode": mode,
            "progress": progress, "parallel": parallel,
        })

    # 1 — a month that pays for itself
    add("cover", "Cover the month",
        "Money coming in has to beat money going out — nothing else holds until it does.",
        done=(avg_net >= 0),
        metric=("about " + _money(avg_net) + "/mo " + ("to spare" if avg_net >= 0 else "short")),
        action="See where the money goes", tab="thismonth", mode="stop_overspend")

    # 2 — a small buffer so a surprise doesn't hit the card
    add("starter", "Set aside a starter cushion",
        "A small buffer means the next surprise goes to savings, not onto an 18% card.",
        done=(savings >= starter_target),
        metric=(_money(savings) + " of " + _money(starter_target) + " saved"
                if savings or spend else "set your savings balance"),
        action="Build the cushion", tab="thismonth", mode="cushion",
        progress=_progress_pct(savings, starter_target))

    # 3 — the most expensive money she owes
    apr_txt = (str(round(card_apr, 2)) + "%") if card_apr else "a high rate"
    add("card", "Wipe out the credit card",
        "At " + apr_txt + " it's the most expensive money you owe — clearing it beats almost any other use of a dollar.",
        done=(card <= 0),
        metric=(_money(card) + " left" if card > 0 else "paid off — nice"),
        action="Attack the card", tab="thismonth", mode="kill_debt")

    # 4 — a real safety net (three months of ESSENTIAL bills, not one-time reno)
    add("safety", "Build a 3-month safety net",
        "Three months of essential bills" + (" (~" + _money(safety_target) + ")" if safety_target else "") +
        " — mortgage, utilities, insurance, groceries — keeps a lost paycheck from undoing the progress above. One-time renovation costs are left out so the target doesn't jump around.",
        done=(safety_target > 0 and savings >= safety_target),
        metric=(_money(savings) + " of " + _money(safety_target) + " saved"
                if safety_target else "add a month of spending first"),
        action="Grow savings", tab="thismonth", mode="cushion",
        progress=_progress_pct(savings, safety_target))

    # 5 — the kitchen goal, funded without over-borrowing
    if reno.get("active"):
        total = float(reno.get("budget_total") or 0)
        can_fund = float(reno.get("can_fund") or 0)
        covered = can_fund >= total and total > 0
        add("kitchen", "Finish the kitchen on budget",
            "It's the big goal — keep it on a real number and let the HELOC cover it without over-borrowing. Runs alongside the debt steps.",
            done=False,
            metric=(_money(total) + " planned" +
                    (" · HELOC can cover it" if covered else
                     (" · HELOC covers " + _money(can_fund)) if can_fund else "")),
            action="Open the kitchen budget", tab="reno",
            progress=_progress_pct(can_fund, total), parallel=True)

    # 6 — the HELOC, once the card is gone
    if heloc > 0 or heloc_apr:
        heloc_apr_txt = (str(round(heloc_apr, 2)) + "%") if heloc_apr else "its rate"
        add("heloc", "Pay down the HELOC",
            "Once the card's gone, the " + heloc_apr_txt + " HELOC is the next most expensive debt — and it frees cash for retirement.",
            done=(heloc <= 0),
            metric=(_money(heloc) + " left" if heloc > 0 else "paid off"),
            action="Plan the payoff", tab="reno")

    # 7 — the retirement date
    if retire.get("configured"):
        year = retire.get("year")
        covered = retire.get("covered")
        surplus = float(retire.get("surplus") or 0)
        add("retire", "Lock in the retirement date",
            "With the debts under control, pension + Social Security decide when she can stop working.",
            done=bool(covered),
            metric=("retire " + str(year) + ": " +
                    (_money(surplus) + "/mo to spare" if surplus >= 0 else _money(-surplus) + "/mo short")),
            action="Open the retirement plan", tab="retire")
    else:
        add("retire", "Lock in the retirement date",
            "With the debts under control, pension + Social Security decide when she can stop working.",
            done=False, metric="build her retirement picture",
            action="Open the retirement plan", tab="retire")

    # The current focus is the first sequential milestone that isn't done.
    # Parallel goals (the kitchen) show progress but never hold the pointer.
    current_idx = next((i for i, s in enumerate(steps)
                        if not s["done"] and not s["parallel"]), None)
    for i, s in enumerate(steps):
        s["status"] = ("done" if s["done"]
                       else "goal" if s["parallel"]
                       else "now" if i == current_idx
                       else "later")
    done_ct = sum(1 for s in steps if s["done"] and not s["parallel"])
    seq_total = sum(1 for s in steps if not s["parallel"])
    current = steps[current_idx] if current_idx is not None else None

    # Human step number counts sequential milestones only (skip the goal).
    current_num = None
    if current_idx is not None:
        current_num = sum(1 for s in steps[:current_idx + 1] if not s["parallel"])

    return {
        "steps": steps,
        "current_key": current["key"] if current else None,
        "current_num": current_num,
        "done_count": done_ct,
        "total": seq_total,
        "all_done": current is None,
    }


# ── Monthly checklist (a concrete routine she can tick off) ────────
def monthly_checklist(ctx):
    """The handful of concrete things to do THIS month, derived from her
    live state — a checkable routine that turns the roadmap's guidance into
    actions. `ctx` carries: current_step (the roadmap's now-step dict or
    None), surplus (monthly money to spare), uncategorized (count),
    reno_active (bool), labor_pending (count), card_balance, heloc_balance.
    Each item: {key, text, detail, tab?, mode?}."""
    ctx = ctx or {}
    items = [{
        "key": "import",
        "text": "Drop in this month's Golden 1 statement",
        "detail": "it reads the balances, bills and debts so every number is current.",
        "tab": "spending",
    }]

    u = int(ctx.get("uncategorized") or 0)
    if u:
        items.append({
            "key": "review",
            "text": f"Sort {u} uncategorized transaction{'s' if u != 1 else ''} into buckets",
            "detail": "a quick tap each on the Spending tab keeps the picture accurate.",
            "tab": "spending",
        })

    step = ctx.get("current_step") or {}
    key = step.get("key")
    surplus = float(ctx.get("surplus") or 0)
    amt = _money(surplus) if surplus > 0 else None
    if key == "card" and ctx.get("card_balance"):
        items.append({"key": "move_card",
                      "text": (f"Pay {amt} on the credit card" if amt else "Put every spare dollar on the credit card"),
                      "detail": "it's the most expensive debt — clear it first.",
                      "tab": "thismonth", "mode": "kill_debt"})
    elif key in ("starter", "safety"):
        items.append({"key": "move_savings",
                      "text": (f"Move {amt} to savings" if amt else "Move whatever you can to savings"),
                      "detail": "building the cushion is this month's focus.",
                      "tab": "thismonth", "mode": "cushion"})
    elif key == "heloc" and ctx.get("heloc_balance"):
        items.append({"key": "move_heloc",
                      "text": (f"Send {amt} extra to the HELOC" if amt else "Send anything extra to the HELOC"),
                      "detail": "clears the renovation debt sooner and frees cash for retirement.",
                      "tab": "reno"})
    elif key == "cover":
        items.append({"key": "trim",
                      "text": "Trim your biggest everyday bucket a little",
                      "detail": "spending is running ahead of income — ease off the top category.",
                      "tab": "thismonth", "mode": "stop_overspend"})

    lp = int(ctx.get("labor_pending") or 0)
    if lp:
        items.append({"key": "labor",
                      "text": f"Tag {lp} contractor payment{'s' if lp != 1 else ''} to the kitchen",
                      "detail": "so the renovation's true cost stays complete.",
                      "tab": "reno"})

    if ctx.get("reno_active"):
        items.append({"key": "quotes",
                      "text": "Log any new kitchen quotes",
                      "detail": "update the line costs and re-lock the plan if you're happy with them.",
                      "tab": "reno"})

    items.append({"key": "balances",
                  "text": "Glance that your balances look right",
                  "detail": "savings and card balances read from the statement — a quick sanity check.",
                  "tab": "plan"})
    return items

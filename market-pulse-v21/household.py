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
    # ── Internal transfers (Golden 1 self-labels these) ──
    ("Transfer", [
        "transfer", "to loan", "from loan", "to share", "from share",
        "online transfer", "moneylink", "overdraft protection",
        "book transfer", "internal transfer", "to savings", "from savings",
        "mobile deposit transfer",
    ]),
    # ── Credit-card payments (counted on the card side, so exclude here) ──
    ("Credit Card Payment", [
        "payment thank you", "autopay", "online payment", "epay",
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
    ]),
    # ── Housing (fixed) ──
    ("Mortgage", [
        "mortgage", "mtg", "home loan", "loan servic", "mr cooper",
        "rocket mortgage", "loancare", "pennymac", "penny mac",
        "freedom mortgage", "caliber home", "wells fargo home",
        "chase mortgage", "flagstar", "newrez", "shellpoint", "carrington",
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
    ("Shopping", [
        "amazon", "amzn", "walmart", "target", "best buy", "home depot",
        "lowe's", "lowes", "ikea", "macy", "nordstrom", "ross store",
        "tj maxx", "tjmaxx", "marshalls", "kohl", "old navy", "the gap",
        "etsy", "ebay", "wayfair", "costco whse", "costco wholesale",
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

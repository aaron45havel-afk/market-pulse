"""Email sign-in links — the way in Google cannot block. Signed, expiring,
single-use tokens; a confirm page (mail scanners open links on their own);
no answer that reveals who is on the access list; links that point only at
this site; and the Google callback's redirect kept on-site. Offline.

Run: python tests/test_email_signin.py
"""
from __future__ import annotations

import asyncio
import os
import sys
from urllib.parse import urlencode

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
os.environ["SESSION_SECRET"] = "test-secret-not-real"
os.environ["ADMIN_EMAILS"] = "owner@example.com"
os.environ["SALES_EMAILS"] = "rep@example.com"
os.environ.pop("PUBLIC_BASE_URL", None)

import auth as A  # noqa: E402

_FAILS: list[str] = []
_COUNT = 0


def check(cond, msg):
    global _COUNT
    _COUNT += 1
    if not cond:
        _FAILS.append(msg)


# ── the token ──────────────────────────────────────────────────────
T0 = 1_800_000_000
tok = A.make_email_link("Owner@Example.com ", now=T0)
d = A.read_email_link(tok, now=T0 + 60)
check(d and d["email"] == "owner@example.com" and d["exp"] == T0 + A.EMAIL_LINK_TTL,
      "a link names one address, lower-cased, and expires in 15 minutes")
check(A.read_email_link(tok, now=T0 + A.EMAIL_LINK_TTL + 1) is None, "an expired link is refused")
p64, s64 = tok.split(".")
forged = A._b64(A._unb64(p64).replace(b"owner@example.com", b"evil@example.com")) + "." + s64
check(A.read_email_link(forged, now=T0) is None, "an edited link fails its signature")
check(A.read_email_link("", now=T0) is None and A.read_email_link("garbage", now=T0) is None
      and A.read_email_link("a.b", now=T0) is None, "junk is refused, not crashed on")
check(A.read_email_link(tok, now=T0) is not None, "READING DOES NOT USE IT UP — the confirm page only reads")
check(A.consume_email_link(tok, now=T0 + 5) == "owner@example.com", "the button uses it")
check(A.consume_email_link(tok, now=T0 + 6) is None and A.read_email_link(tok, now=T0 + 6) is None,
      "ONCE: a used link signs no one in again")

# Two clicks at once: both reads finish before either records the use.
race = A.make_email_link("owner@example.com", now=T0)
_real_read = A.read_email_link
_snapshot = _real_read(race, now=T0)
A.read_email_link = lambda token, now=None: dict(_snapshot)
try:
    first, second = A.consume_email_link(race, now=T0), A.consume_email_link(race, now=T0)
finally:
    A.read_email_link = _real_read
check(first == "owner@example.com" and second is None,
      "TWO CLICKS RACING: the use is recorded under the lock, so only one of them signs in")

sess = A.make_session("owner@example.com", "admin")
check(A.read_email_link(sess) is None, "a session cookie is not a sign-in link")
check(A.verify_session(A.make_email_link("owner@example.com")) is None,
      "and a sign-in link is not a session cookie — each is signed under its own context")
saved = os.environ.pop("SESSION_SECRET")
check(A.make_email_link("owner@example.com") == "" and A.read_email_link(tok) is None,
      "no SESSION_SECRET: no links made or accepted")
os.environ["SESSION_SECRET"] = saved
check(A.make_email_link("") == "", "no address, no link")

# ── the routes ─────────────────────────────────────────────────────
from starlette.requests import Request  # noqa: E402

import crm  # noqa: E402
import main  # noqa: E402


def req(method, path, form=None, headers=None, query=""):
    body = urlencode(form or {}).encode()
    hdrs = {"host": "app.example.test", "content-type": "application/x-www-form-urlencoded", **(headers or {})}
    scope = {"type": "http", "method": method, "path": path, "raw_path": path.encode(),
             "query_string": query.encode(), "headers": [(k.lower().encode(), v.encode()) for k, v in hdrs.items()],
             "scheme": "https", "server": ("app.example.test", 443), "client": ("203.0.113.9", 5555),
             "root_path": "", "app": main.app}

    async def receive():
        return {"type": "http.request", "body": body, "more_body": False}
    return Request(scope, receive)


sent: list[dict] = []
crm.send_via_resend = lambda **kw: sent.append(kw) or {"ok": True}
main._EMAIL_LINK_SENT.clear()

r = asyncio.run(main.auth_email_start(req("POST", "/auth/email/start",
                                          {"email": "owner@example.com", "redirect": "/capital"})))
body = r.body.decode()
check(r.status_code == 200 and "Check your email" in body, "the owner is told to check their email")
check(len(sent) == 1 and sent[0]["to_email"] == "owner@example.com", "and a link is mailed to that address")
link = sent[0]["body"].split("\n\n")[1] if sent else ""
check(link.startswith("https://app.example.test/auth/email/verify?t=") and "redirect=/capital" in link,
      "the link points at this site and carries where to go after")

r2 = asyncio.run(main.auth_email_start(req("POST", "/auth/email/start", {"email": "stranger@example.com"})))
check(len(sent) == 1, "AN ADDRESS NOT ON THE LIST GETS NOTHING")
check(r2.status_code == 200 and "Check your email" in r2.body.decode(),
      "and the page says exactly the same — it never reveals who is on the list")

sent.clear()
main._EMAIL_LINK_SENT.clear()
evil = asyncio.run(main.auth_email_start(req("POST", "/auth/email/start", {"email": "owner@example.com"},
                                             headers={"x-forwarded-host": "attacker.example"})))
check(sent and "attacker.example" not in sent[0]["body"] and "app.example.test" in sent[0]["body"],
      "A FORGED X-Forwarded-Host CANNOT POINT A GENUINE LINK AT SOMEONE ELSE'S SERVER")
os.environ["PUBLIC_BASE_URL"] = "https://pulse.example.com/"
check(main._site_base(req("GET", "/")) == "https://pulse.example.com", "PUBLIC_BASE_URL wins when set")
os.environ.pop("PUBLIC_BASE_URL")

main._EMAIL_LINK_SENT.clear()
sent.clear()
for _ in range(5):
    asyncio.run(main.auth_email_start(req("POST", "/auth/email/start", {"email": "owner@example.com"})))
check(len(sent) == main.EMAIL_LINK_MAX_PER_ADDRESS,
      f"at most {main.EMAIL_LINK_MAX_PER_ADDRESS} links per address per 15 minutes (got {len(sent)})")
check(main._email_link_rate_ok("k", 1, now=0) and not main._email_link_rate_ok("k", 1, now=10)
      and main._email_link_rate_ok("k", 1, now=1000), "the limit is a sliding window")

main._EMAIL_LINK_SENT.clear()
sent.clear()
crm.send_via_resend = lambda **kw: {"ok": False, "error": "RESEND_API_KEY not set"}
down = asyncio.run(main.auth_email_start(req("POST", "/auth/email/start", {"email": "owner@example.com"})))
check(down.status_code == 200 and "admin token sign-in" in down.body.decode(),
      "if mail cannot be sent, the page still points to the admin-token way in")

good = A.make_email_link("owner@example.com")
page = asyncio.run(main.auth_email_verify_page(req("GET", "/auth/email/verify"), t=good, redirect="/capital"))
pb = page.body.decode()
check("Sign in to Market Pulse as <b>owner@example.com</b>" in pb and 'action="/auth/email/verify"' in pb,
      "THE LINK OPENS A CONFIRM BUTTON, it does not sign in — a mail scanner's visit uses nothing")
check(A.read_email_link(good) is not None, "and opening the page leaves the link unused")

done = asyncio.run(main.auth_email_verify(req("POST", "/auth/email/verify", {"t": good, "redirect": "/capital"})))
cookie = done.headers.get("set-cookie", "")
check(done.status_code == 303 and done.headers["location"] == "/capital", "the button signs in and goes on to /capital")
check(cookie.startswith("mp_session=") and "httponly" in cookie.lower() and "secure" in cookie.lower(),
      "with the same 30-day, HttpOnly, Secure session Google sign-in sets")
sess_val = cookie.split(";")[0].split("=", 1)[1]
check((A.verify_session(sess_val) or {}).get("role") == "admin", "the owner is an admin")
again = asyncio.run(main.auth_email_verify(req("POST", "/auth/email/verify", {"t": good, "redirect": "/capital"})))
check(again.status_code == 400 and "expired" in again.body.decode(), "pressing it twice does nothing the second time")

rep = asyncio.run(main.auth_email_verify(req("POST", "/auth/email/verify",
                                             {"t": A.make_email_link("rep@example.com"), "redirect": "/pipeline"})))
rv = rep.headers.get("set-cookie", "").split(";")[0].split("=", 1)[1]
check((A.verify_session(rv) or {}).get("role") == "sales", "a sales address gets the sales role, not admin")

os.environ["ADMIN_EMAILS"] = ""
revoked = asyncio.run(main.auth_email_verify(req("POST", "/auth/email/verify",
                                                 {"t": A.make_email_link("owner@example.com")})))
check(revoked.status_code == 400, "an address taken off the list between sending and clicking is refused")
os.environ["ADMIN_EMAILS"] = "owner@example.com"

off = asyncio.run(main.auth_email_verify(req("POST", "/auth/email/verify",
                                             {"t": A.make_email_link("owner@example.com"), "redirect": "//evil.example"})))
check(off.headers["location"] == "/", "an off-site redirect is replaced with the home page")
bad = asyncio.run(main.auth_email_verify_page(req("GET", "/auth/email/verify"), t="nope", redirect="/"))
check("That link has expired" in bad.body.decode(), "a bad link says so and offers a new one")

# ── the sign-in page and the doors to it ───────────────────────────
si = asyncio.run(main.sign_in_page(req("GET", "/sign-in"), redirect="/capital"))
sb = si.body.decode()
check('action="/auth/email/start"' in sb and 'value="/capital"' in sb and "Admin token sign-in" in sb,
      "the sign-in page offers the email link next to Google and the admin token")
anon = asyncio.run(main.capital_page(req("GET", "/capital")))
check(anon.status_code == 303 and anon.headers["location"] == "/sign-in?redirect=/capital",
      "/capital sends a signed-out visitor to the full sign-in page, every way in on it")
src = open(os.path.join(ROOT, "main.py")).read()
check('redirect_to = _safe_redirect(request.cookies.get(OAUTH_REDIRECT_COOKIE, "/pipeline"))' in src,
      "GOOGLE'S CALLBACK KEEPS ITS REDIRECT ON-SITE — '//elsewhere' starts with a slash too")
check(main._safe_redirect("//evil.example") == "/" and main._safe_redirect("/capital") == "/capital",
      "the shared helper refuses protocol-relative redirects")

if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for x in _FAILS:
        print("  ✗", x)
    sys.exit(1)
print(f"OK — all {_COUNT} email sign-in checks passed.")

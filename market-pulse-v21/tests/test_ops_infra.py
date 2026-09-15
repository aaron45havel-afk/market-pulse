"""Storage and comms — the adapters, and what they refuse.

Run:  python tests/test_ops_infra.py      (exit 0 = all pass)

Pure: a temporary directory and a fake provider, no database and no
network. Both modules are mostly about refusals, so that is what is
tested — a storage layer that stores and a comms layer that sends are
the easy halves, and neither is where the damage comes from.
"""
import os
import sys
import tempfile
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from lib.ops import clock as C
from lib.ops import storage as ST
from lib.ops.integrations import comms as CM

_COUNT = 0
_FAILS = []


def check(cond, msg):
    global _COUNT
    _COUNT += 1
    if not cond:
        _FAILS.append(msg)


def raises(exc, fn, *a, **k):
    try:
        fn(*a, **k)
        return False
    except exc:
        return True


NOW = datetime(2026, 8, 27, 12, 0, tzinfo=timezone.utc)
TS = C.TimeService(NOW)
SECRET = "a-signing-secret-for-tests"

tmp = tempfile.TemporaryDirectory()
store = ST.LocalStorage(tmp.name, SECRET, ts=TS)


# ══════════════════════════════════════════════════════════════════
# storage
# ══════════════════════════════════════════════════════════════════
data = b"move-in photo bytes, kitchen, 2026-08-27"
meta = store.put("leases/17/movein/kitchen.jpg", data, "image/jpeg")
check(meta["sha256"] == ST.sha256_of(data), "put records a SHA-256")
check(meta["size_bytes"] == len(data), "and the size")
check(store.get("leases/17/movein/kitchen.jpg") == data,
      "and the bytes come back")
check(store.exists("leases/17/movein/kitchen.jpg"), "exists() says so")
check(not store.exists("leases/17/movein/nothing.jpg"),
      "and not for something absent")

check(store.get("leases/17/movein/kitchen.jpg",
                expect_sha256=meta["sha256"]) == data,
      "a get with the recorded hash succeeds")
p = os.path.join(tmp.name, "leases/17/movein/kitchen.jpg")
with open(p, "wb") as f:
    f.write(b"tampered")
check(raises(ST.StorageError, store.get, "leases/17/movein/kitchen.jpg",
             expect_sha256=meta["sha256"]),
      "BUT A FILE THAT NO LONGER MATCHES ITS RECORDED HASH RAISES rather "
      "than returning the bytes. A move-in photo decides a deposit "
      "dispute months later, and 'this is the file we stored that day' "
      "has to be checkable rather than assertable")
check(store.get("leases/17/movein/kitchen.jpg") == b"tampered",
      "the bytes are still readable without the hash — the check is the "
      "caller's to make, and it is made wherever the hash was recorded")

# ── key safety ──
for bad in ("../../etc/passwd", "/etc/passwd", "a\\b", "leases//17",
            "", "  ", "a" * 600, "leases/../../out"):
    check(raises(ST.StorageError, ST.check_key, bad),
          f"the key {bad!r} is refused rather than normalised — "
          f"normalising guesses at what the caller meant, and the guess "
          f"is wrong exactly when it matters")
check(ST.check_key("leases/17/doc.pdf") == "leases/17/doc.pdf",
      "an ordinary key passes through")
check(raises(ST.StorageError, store.put, "../escape.txt", b"x"),
      "and put refuses one too, not just the standalone validator")
check(raises(ST.StorageError, store.put, "ok/text.txt", "a string"),
      "put takes bytes, not text — an encoding guessed here is an "
      "encoding wrong later")

# ── signed URLs ──
url = store.signed_url("leases/17/doc.pdf")
check("signature=" in url and "expires=" in url,
      "a signed URL carries a signature and an expiry")
check("leases/17/doc.pdf" in url or "leases%2F17%2Fdoc.pdf" in url,
      "and names the object")

expires = int(url.split("expires=")[1].split("&")[0])
sig = url.split("signature=")[1].split("&")[0]
check(store.verify_url("leases/17/doc.pdf", expires, sig),
      "and verifies")
check(not store.verify_url("leases/17/doc.pdf", expires, sig[:-1] + "0"),
      "A TAMPERED SIGNATURE FAILS")
check(not store.verify_url("leases/99/other.pdf", expires, sig),
      "AND THE SIGNATURE IS BOUND TO THE KEY — it cannot be moved to "
      "another object, which is the whole difference between a signed "
      "URL and an obscure one")
check(not store.verify_url("leases/17/doc.pdf", expires + 3600, sig),
      "nor to a later expiry, so a link cannot be extended by editing it")

_later = ST.LocalStorage(tmp.name, SECRET, ts=C.TimeService(
    NOW + timedelta(hours=1)))
check(not _later.verify_url("leases/17/doc.pdf", expires, sig),
      "AND IT EXPIRES. A document link that outlives its purpose is a "
      "credential sitting in a browser history")

_wrong_secret = ST.LocalStorage(tmp.name, "a-different-secret", ts=TS)
check(not _wrong_secret.verify_url("leases/17/doc.pdf", expires, sig),
      "a signature from a different key does not verify")

check(raises(ST.StorageError, store.signed_url, "k.pdf",
             ttl=timedelta(days=3)),
      "a multi-day link is refused; past the ceiling a URL stops being a "
      "link for one person right now")
check(raises(ST.StorageError, store.signed_url, "k.pdf",
             ttl=timedelta(seconds=-1)),
      "and an already-expired one is not useful")
check(raises(ST.StorageError, ST.LocalStorage, tmp.name, ""),
      "STORAGE REFUSES TO CONSTRUCT WITHOUT A SIGNING SECRET. A "
      "predictable signing key is worse than none, because the URLs look "
      "signed")

# ── S3 presigning is computed locally and deterministically ──
s3 = ST.S3Storage("bucket", "us-west-2", "AKIAEXAMPLE", "secretexample",
                  ts=TS)
u1 = s3.signed_url("leases/17/doc.pdf")
check("X-Amz-Signature=" in u1 and "X-Amz-Algorithm=AWS4-HMAC-SHA256" in u1,
      "the S3 backend produces a SigV4 presigned URL")
check("X-Amz-Expires=900" in u1, "with the requested expiry in seconds")
check(s3.signed_url("leases/17/doc.pdf") == u1,
      "and it is deterministic for a fixed clock — no network call, which "
      "is why this needs no boto3")
check(s3.signed_url("leases/18/doc.pdf") != u1,
      "a different object signs differently")
check(raises(ST.StorageError, ST.S3Storage, "b", "r", "", "s"),
      "S3Storage refuses to construct without credentials rather than "
      "failing on first use, when the caller is further from the cause")

_r2 = ST.S3Storage("b", "auto", "k", "s",
                   endpoint="https://acct.r2.cloudflarestorage.com/b", ts=TS)
check(_r2.signed_url("x.pdf").startswith(
        "https://acct.r2.cloudflarestorage.com/b/x.pdf"),
      "a non-AWS S3-compatible endpoint is honoured, which is what makes "
      "R2 or B2 a config change rather than a rewrite")

# ── from_env refuses to invent a secret ──
_saved = {k: os.environ.pop(k, None) for k in
          ("MF_S3_BUCKET", "MF_STORAGE_SECRET", "SECRET_KEY",
           "MF_STORAGE_ROOT")}
check(raises(ST.StorageError, ST.from_env),
      "with nothing configured, from_env RAISES rather than inventing a "
      "signing key — an unsigned document URL that looks signed is worse "
      "than an error")
os.environ["MF_STORAGE_SECRET"] = SECRET
os.environ["MF_STORAGE_ROOT"] = tmp.name
check(isinstance(ST.from_env(), ST.LocalStorage),
      "with a secret it falls back to local disk (and logs a warning that "
      "this is not durable enough for documents that decide disputes)")
for k, v in _saved.items():
    if v is None:
        os.environ.pop(k, None)
    else:
        os.environ[k] = v


# ══════════════════════════════════════════════════════════════════
# comms
# ══════════════════════════════════════════════════════════════════
fake = CM.FakeComms()
msg = CM.Message("email", "resident@example.invalid", "Rent receipt",
                 "We received your payment of $1,500.00.", "rent_receipt",
                 user_id=7)
result = fake.send(msg)
check(result["ok"] and len(fake.sent) == 1,
      "the fake records a send instead of making one")
check(fake.to_address("resident@example.invalid")[0]["reason"]
      == "rent_receipt",
      "and it is queryable by address — a test proves a reminder went out "
      "by looking here, which is the same question the audit log answers "
      "in production")

check(raises(CM.CommsError, fake.send,
             CM.Message("email", "resident@example.invalid", "Hi",
                        "body", "special_offer")),
      "AN UNKNOWN REASON IS REFUSED. Free text turns 'what did we send "
      "this person' into an unanswerable question within a year")
check(raises(CM.CommsError, fake.send,
             CM.Message("email", "not-an-address", "Hi", "body",
                        "account")),
      "a malformed email address is refused")
check(raises(CM.CommsError, fake.send,
             CM.Message("email", "a@b.com", "", "body", "account")),
      "an email with no subject is refused — a blank one is filed as spam")
check(raises(CM.CommsError, fake.send,
             CM.Message("email", "a@b.com", "Subject", "   ", "account")),
      "and an EMPTY BODY, which reads to the recipient as a system fault "
      "and proves nothing if produced as a record")
check(raises(CM.CommsError, fake.send,
             CM.Message("pigeon", "a@b.com", "s", "b", "account")),
      "an unknown channel is refused")

check(fake.send(CM.Message("sms", "+14155550123", "", "Your code is 123456",
                           "verification"))["ok"],
      "an E.164 number is accepted for SMS")
for bad in ("4155550123", "+0155550123", "555-0123", "+1 415 555 0123", ""):
    check(raises(CM.CommsError, fake.send,
                 CM.Message("sms", bad, "", "body", "account")),
          f"but {bad!r} is not E.164 — a number without a country code is "
          f"ambiguous, and the ambiguity resolves differently per carrier, "
          f"sometimes to somebody else's phone")
check(raises(CM.CommsError, fake.send,
             CM.Message("sms", "+14155550123", "", "x" * 2000, "account")),
      "and a 2000-character SMS is refused rather than silently split "
      "into ten-plus billed segments")

check(raises(CM.CommsError, fake.send,
             CM.Message("email", "boom@example.invalid", "s", "b",
                        "account")) is False,
      "an ordinary address sends")
fake.fail_on.add("boom@example.invalid")
check(raises(CM.CommsError, fake.send,
             CM.Message("email", "boom@example.invalid", "s", "b",
                        "account")),
      "and a forced failure raises, so the retry path is exercised by "
      "tests rather than by an outage")

null = CM.NullComms()
check(raises(CM.CommsError, null.send,
             CM.Message("email", "a@b.com", "s", "body", "rent_notice")),
      "NullComms RAISES rather than dropping the message. A rent reminder "
      "that silently does not go out is worse than an error — the error "
      "is noticed in a day, the silence at the hearing")

_saved_provider = os.environ.pop("MF_COMMS_PROVIDER", None)
check(isinstance(CM.from_env(), CM.NullComms),
      "with nothing configured, from_env gives NullComms — a fake that "
      "stands in for a real provider by default is how a month of "
      "notices goes nowhere while every test passes")
check(isinstance(CM.from_env(fake_ok=True), CM.FakeComms),
      "and the fake has to be asked for deliberately")
os.environ["MF_COMMS_PROVIDER"] = "twilio"
check(raises(CM.CommsError, CM.from_env),
      "an unimplemented provider raises rather than falling back to a "
      "fake that would look like it worked")
if _saved_provider is None:
    os.environ.pop("MF_COMMS_PROVIDER", None)
else:
    os.environ["MF_COMMS_PROVIDER"] = _saved_provider


# ══════════════════════════════════════════════════════════════════
# structured logging
# ══════════════════════════════════════════════════════════════════
import io
import json as _json
import logging as _logging

from lib.ops import obs

_buf = io.StringIO()
_h = _logging.StreamHandler(_buf)
_h.setFormatter(obs.JsonFormatter())
_log = _logging.getLogger("mf.test.obs")
_log.handlers = [_h]
_log.setLevel(_logging.INFO)
_log.propagate = False

obs.bind(request_id="abc123def456", user_id=7, portal="staff")
obs.log_with(_log, _logging.INFO, "scoped read", entity="mf_users", rows=3)
line = _json.loads(_buf.getvalue().strip().splitlines()[-1])
check(line["msg"] == "scoped read" and line["level"] == "INFO",
      "a log line is one JSON object with its message and level")
check(line["request_id"] == "abc123def456",
      "CARRYING THE REQUEST ID. Without it, the access log line, the "
      "audit row, the job and the exception are four searches with no way "
      "to know they matched")
check(line["user_id"] == 7 and line["portal"] == "staff",
      "and who was asking, through which portal")
check(line["entity"] == "mf_users" and line["rows"] == "3",
      "with the structured fields the call site passed")

_buf.truncate(0), _buf.seek(0)
obs.log_with(_log, _logging.INFO, "login", email="a@b.com",
             password="hunter2-hunter2", mfa_secret="JBSWY3DPEHPK3PXP",
             session_token="abc")
line = _json.loads(_buf.getvalue().strip().splitlines()[-1])
check("hunter2-hunter2" not in _buf.getvalue()
      and line["password"] == "[redacted]",
      "A FIELD WHOSE NAME LOOKS LIKE A CREDENTIAL IS REDACTED, not "
      "truncated and not hashed — a truncated token still leaks bits, and "
      "a hash of a six-digit code is a six-digit code to anyone with a "
      "laptop")
check(line["mfa_secret"] == "[redacted]"
      and line["session_token"] == "[redacted]",
      "TOTP seeds and session tokens too")
check(line["email"] == "a@b.com",
      "while an ordinary field is logged as itself — a redactor that "
      "eats everything gets turned off")

check(obs.redact("x" * 900).endswith("[truncated]"),
      "a very long value is truncated, so one bad field cannot push a "
      "day of logs out of the retention window")


class _Unserialisable:
    def __repr__(self):
        return "<odd object>"


_buf.truncate(0), _buf.seek(0)
obs.log_with(_log, _logging.INFO, "odd", thing=_Unserialisable())
check("<odd object>" in _buf.getvalue(),
      "AND AN UNSERIALISABLE VALUE DEGRADES TO ITS REPR RATHER THAN "
      "RAISING. A formatter that can throw loses exactly the message you "
      "needed")

check(obs.new_request_id() != obs.new_request_id(),
      "request ids are unique")
check(len(obs.new_request_id()) == 16, "and short enough to quote by hand")

tmp.cleanup()

if _FAILS:
    print(f"FAIL — {len(_FAILS)}/{_COUNT} checks failed:")
    for m in _FAILS:
        print("  ✗", m)
    sys.exit(1)
print(f"OK — all {_COUNT} ops-infra checks passed.")
print("   Signatures bound to key and expiry, a tampered file refused "
      "against its hash,\n   and nothing sent when no provider is "
      "configured.")
sys.exit(0)

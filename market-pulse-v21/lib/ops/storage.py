"""Object storage behind signed, expiring URLs — with a fake that is honest.

ARCHITECTURE.md §4: "Railway volumes are not durable enough for lead
certificates and move-in photos that decide deposit disputes. Use
S3-compatible storage behind lib/ops/storage.py, signed expiring URLs
only, no public bucket, SHA-256 recorded at upload for tamper evidence."

Two backends:

  S3Storage      real, AWS SigV4 signed URLs computed locally. No boto3 —
                 presigning is an HMAC chain over a canonical request and
                 needs no network, so the dependency would buy nothing.
  LocalStorage   files on disk with the SAME signing scheme, for
                 development and tests.

THE FAKE SIGNS FOR REAL. A development backend that returned an
unsigned path would mean signature expiry, tampering and the URL format
were never exercised until production — and those are the parts that
matter. LocalStorage differs from S3Storage in where the bytes go and
nothing else.

The SHA-256 is not a checksum for corruption. A move-in photo decides a
deposit dispute months later, and "this is the file we stored that day"
has to be checkable rather than assertable. put() computes it, returns
it, and refuses a get() whose bytes no longer match.
"""
from __future__ import annotations

import hashlib
import hmac
import logging
import os
import re
from datetime import timedelta
from pathlib import Path
from urllib.parse import quote, urlencode

from lib.ops import clock as C

log = logging.getLogger("mf.storage")

DEFAULT_TTL = timedelta(minutes=15)
MAX_TTL = timedelta(hours=12)
# Anything longer stops being "a link for this person right now" and
# becomes a credential that leaks through a browser history or a
# forwarded email.

SAFE_KEY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]{0,511}$")


class StorageError(RuntimeError):
    pass


def check_key(key: str) -> str:
    """Reject anything that could escape the prefix it was given.

    `..` is the obvious one. A leading slash and a backslash are the two
    that get forgotten, and on a local backend either turns a key into
    an absolute path anywhere the process can write.
    """
    if not key or not isinstance(key, str):
        raise StorageError("a storage key is required")
    if ".." in key or key.startswith("/") or "\\" in key or "//" in key:
        raise StorageError(
            f"unsafe storage key {key!r} — traversal, absolute paths and "
            f"empty segments are refused rather than normalised, because "
            f"normalising guesses at what the caller meant")
    if not SAFE_KEY.match(key):
        raise StorageError(f"storage key {key!r} has characters outside "
                           f"[A-Za-z0-9._/-]")
    return key


def sha256_of(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class _Signer:
    """HMAC-SHA256 over (key, expiry, method). Shared by both backends."""

    def __init__(self, secret: str):
        if not secret:
            raise StorageError(
                "storage signing needs a secret. Refusing to fall back to a "
                "default: a predictable signing key means anybody can mint "
                "a URL for any object, which is worse than no signing at "
                "all because it looks signed.")
        self._secret = secret.encode()

    def sign(self, key: str, expires_at: int, method: str = "GET") -> str:
        msg = f"{method.upper()}\n{key}\n{expires_at}".encode()
        return hmac.new(self._secret, msg, hashlib.sha256).hexdigest()

    def verify(self, key: str, expires_at: int, signature: str,
               now_epoch: int, method: str = "GET") -> bool:
        if now_epoch > expires_at:
            return False
        return hmac.compare_digest(self.sign(key, expires_at, method),
                                   signature or "")


class LocalStorage:
    """Files under a root directory. Same signing, different bytes."""

    def __init__(self, root, secret: str, ts: C.TimeService | None = None,
                 base_url: str = "/ops/files"):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.signer = _Signer(secret)
        self.ts = ts or C.TimeService()
        self.base_url = base_url.rstrip("/")

    def _path(self, key: str) -> Path:
        p = (self.root / check_key(key)).resolve()
        root = self.root.resolve()
        # Belt and braces with check_key. A symlink inside the root could
        # still point outside it, and this is the check that catches that
        # rather than the pattern match.
        if not str(p).startswith(str(root) + os.sep):
            raise StorageError(f"{key!r} resolves outside the storage root")
        return p

    def put(self, key: str, data: bytes, content_type: str = "") -> dict:
        if not isinstance(data, bytes):
            raise StorageError("storage takes bytes, not text — an encoding "
                               "guessed here is an encoding wrong later")
        p = self._path(key)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)
        digest = sha256_of(data)
        log.info("stored %s (%s bytes)", key, len(data))
        return {"key": key, "sha256": digest, "size_bytes": len(data),
                "content_type": content_type}

    def get(self, key: str, expect_sha256: str | None = None) -> bytes:
        p = self._path(key)
        if not p.exists():
            raise StorageError(f"no object at {key!r}")
        data = p.read_bytes()
        if expect_sha256 and sha256_of(data) != expect_sha256:
            raise StorageError(
                f"{key!r} does not match the hash recorded at upload. Either "
                f"the file changed or the record did; both are a problem, "
                f"and returning the bytes anyway would hide it.")
        return data

    def delete(self, key: str) -> bool:
        p = self._path(key)
        if p.exists():
            p.unlink()
            return True
        return False

    def exists(self, key: str) -> bool:
        return self._path(key).exists()

    def signed_url(self, key: str, ttl: timedelta = DEFAULT_TTL,
                   method: str = "GET") -> str:
        return _signed_url(self.signer, self.base_url, key, ttl, method,
                           self.ts)

    def verify_url(self, key: str, expires: int, signature: str,
                   method: str = "GET") -> bool:
        return self.signer.verify(key, int(expires), signature,
                                  int(self.ts.now().timestamp()), method)


def _signed_url(signer, base, key, ttl, method, ts) -> str:
    check_key(key)
    if ttl > MAX_TTL:
        raise StorageError(
            f"a {ttl} link is refused; the ceiling is {MAX_TTL}. Past that "
            f"a URL stops being a link for one person right now and becomes "
            f"a credential that leaks through a browser history or a "
            f"forwarded email.")
    if ttl <= timedelta(0):
        raise StorageError("a link that has already expired is not useful")
    expires = int((ts.now() + ttl).timestamp())
    sig = signer.sign(key, expires, method)
    return f"{base}/{quote(key)}?" + urlencode({"expires": expires,
                                                "signature": sig})


class S3Storage:
    """S3-compatible storage with locally-computed SigV4 presigned URLs.

    No boto3. Presigning is an HMAC chain over a canonical request — it
    needs no network call and no credentials beyond the ones already in
    the environment, so the dependency would buy nothing but a wheel to
    keep pinned. Uploads go through the job worker rather than the web
    process, which is why only presigning lives here for now.
    """

    def __init__(self, bucket: str, region: str, access_key: str,
                 secret_key: str, endpoint: str | None = None,
                 ts: C.TimeService | None = None):
        if not all((bucket, region, access_key, secret_key)):
            raise StorageError("S3Storage needs bucket, region and "
                               "credentials; refusing to construct a client "
                               "that will fail on first use")
        self.bucket, self.region = bucket, region
        self.access_key, self.secret_key = access_key, secret_key
        # R2 and B2 are S3-compatible on a different host. Defaulting to
        # AWS and letting the caller override is the whole difference.
        self.endpoint = (endpoint or
                         f"https://{bucket}.s3.{region}.amazonaws.com"
                         ).rstrip("/")
        self.ts = ts or C.TimeService()

    def _sigv4_key(self, date_stamp: str) -> bytes:
        k = ("AWS4" + self.secret_key).encode()
        for part in (date_stamp, self.region, "s3", "aws4_request"):
            k = hmac.new(k, part.encode(), hashlib.sha256).digest()
        return k

    def signed_url(self, key: str, ttl: timedelta = DEFAULT_TTL,
                   method: str = "GET") -> str:
        check_key(key)
        if ttl > MAX_TTL:
            raise StorageError(f"a {ttl} link is refused; ceiling {MAX_TTL}")
        now = self.ts.now()
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        date_stamp = now.strftime("%Y%m%d")
        host = self.endpoint.split("://", 1)[1]
        credential = (f"{self.access_key}/{date_stamp}/{self.region}/s3/"
                      f"aws4_request")
        params = {
            "X-Amz-Algorithm": "AWS4-HMAC-SHA256",
            "X-Amz-Credential": credential,
            "X-Amz-Date": amz_date,
            "X-Amz-Expires": str(int(ttl.total_seconds())),
            "X-Amz-SignedHeaders": "host",
        }
        canonical_qs = urlencode(sorted(params.items()), quote_via=quote)
        canonical = "\n".join([
            method.upper(), "/" + quote(key), canonical_qs,
            f"host:{host}\n", "host", "UNSIGNED-PAYLOAD"])
        scope = f"{date_stamp}/{self.region}/s3/aws4_request"
        to_sign = "\n".join([
            "AWS4-HMAC-SHA256", amz_date, scope,
            hashlib.sha256(canonical.encode()).hexdigest()])
        signature = hmac.new(self._sigv4_key(date_stamp), to_sign.encode(),
                             hashlib.sha256).hexdigest()
        return (f"{self.endpoint}/{quote(key)}?{canonical_qs}"
                f"&X-Amz-Signature={signature}")


def from_env(ts: C.TimeService | None = None):
    """The configured backend, or a local one. Never a public bucket.

    Falls back to LocalStorage when S3 is not configured so development
    works, and logs that it did — a silent fallback to local disk in
    production would put deposit-dispute evidence on an ephemeral volume,
    which is the exact failure ARCHITECTURE.md §4 rules out.
    """
    bucket = os.getenv("MF_S3_BUCKET")
    if bucket:
        return S3Storage(bucket, os.getenv("MF_S3_REGION", "auto"),
                         os.getenv("MF_S3_ACCESS_KEY", ""),
                         os.getenv("MF_S3_SECRET_KEY", ""),
                         os.getenv("MF_S3_ENDPOINT"), ts=ts)
    secret = os.getenv("MF_STORAGE_SECRET") or os.getenv("SECRET_KEY")
    if not secret:
        raise StorageError(
            "no storage configured and no MF_STORAGE_SECRET to sign local "
            "URLs with. Refusing to invent one: an unsigned or "
            "predictably-signed document URL is worse than an error, "
            "because it looks like it works.")
    root = os.getenv("MF_STORAGE_ROOT", "/tmp/mfops-storage")
    log.warning("object storage is LOCAL DISK at %s — fine for development, "
                "not durable enough for documents that decide disputes",
                root)
    return LocalStorage(root, secret, ts=ts)

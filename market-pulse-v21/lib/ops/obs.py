"""Structured logs with a request id running through them.

Named obs.py, not logging.py. A module called `logging` inside a package
is a trap that costs somebody an afternoon the first time a sibling
module does `import logging` and gets the wrong one.

The problem this solves is specific. When a resident says "I never got
the notice", the answer lives in four places: the access log line for
their request, the audit row for the read, the job that sent the
message, and the exception if it failed. Without a shared identifier
those are four searches with no way to know they matched. With one they
are `grep`.

  * request_id is a CONTEXTVAR, so it follows the request through
    handlers and awaits without being threaded through every signature.
    A parameter would be dropped somewhere within a month.
  * The same id goes on the mf_audit_log row and into the X-Request-ID
    response header, so an operator can read it off a support email and
    find everything.
  * JSON output, because Railway's log view is a text box and structured
    search over it is the only thing that makes a bad hour tractable.

WHAT IS NEVER LOGGED: a password, a session token, a TOTP secret, or a
message body. The formatter cannot enforce that — it does not know what
a field means — so `redact()` is provided for the call sites that handle
credentials, and tests/test_ops_auth.py checks the audit log for the
literal test password rather than trusting the discipline.
"""
from __future__ import annotations

import contextvars
import json
import logging
import uuid

request_id_var = contextvars.ContextVar("mf_request_id", default="")
user_id_var = contextvars.ContextVar("mf_user_id", default=None)
portal_var = contextvars.ContextVar("mf_portal", default="")

SENSITIVE = ("password", "passwd", "secret", "token", "mfa", "totp",
             "authorization", "cookie", "session", "api_key", "apikey")


def new_request_id() -> str:
    return uuid.uuid4().hex[:16]


def bind(request_id: str = "", user_id=None, portal: str = ""):
    """Attach identity to everything logged for the rest of this context."""
    if request_id:
        request_id_var.set(request_id)
    if user_id is not None:
        user_id_var.set(user_id)
    if portal:
        portal_var.set(portal)


def current_request_id() -> str:
    return request_id_var.get()


def redact(value, key: str = "") -> str:
    """A value safe to log. Not clever — deliberately.

    Anything whose NAME suggests a credential becomes a fixed marker
    rather than a truncation or a hash. A truncated token still leaks
    bits, and a hash of a six-digit TOTP code is a six-digit TOTP code to
    anybody with a laptop.
    """
    if key and any(s in key.lower() for s in SENSITIVE):
        return "[redacted]"
    text = str(value)
    return text if len(text) <= 500 else text[:500] + "…[truncated]"


def scrub(mapping: dict) -> dict:
    return {k: redact(v, k) for k, v in (mapping or {}).items()}


class JsonFormatter(logging.Formatter):
    """One JSON object per line, with the request context folded in."""

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        rid = request_id_var.get()
        if rid:
            payload["request_id"] = rid
        uid = user_id_var.get()
        if uid is not None:
            payload["user_id"] = uid
        portal = portal_var.get()
        if portal:
            payload["portal"] = portal
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        for key, value in getattr(record, "extra_fields", {}).items():
            payload[key] = redact(value, key)
        # default=str so an unserialisable value degrades to its repr
        # rather than making the log line disappear — a formatter that
        # can raise is a formatter that loses exactly the message you
        # needed.
        return json.dumps(payload, default=str)


def configure(level=logging.INFO, json_output: bool = True) -> None:
    """Point the mf.* loggers at a structured handler. Idempotent.

    Only touches `mf` and below. Reconfiguring the root logger would
    change the output format of the analysis boards sharing this process,
    which is not this module's business.
    """
    root = logging.getLogger("mf")
    root.setLevel(level)
    for h in list(root.handlers):
        root.removeHandler(h)
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter() if json_output
                         else logging.Formatter(
                             "%(asctime)s %(levelname)s %(name)s %(message)s"))
    root.addHandler(handler)
    # Do not also bubble to the root handler; that would print every ops
    # line twice, once structured and once not.
    root.propagate = False


def log_with(logger, level, msg, **fields):
    """logger.info(msg) plus structured fields, scrubbed."""
    logger.log(level, msg, extra={"extra_fields": scrub(fields)})

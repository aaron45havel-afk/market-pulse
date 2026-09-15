"""Email and SMS, behind one interface, with a fake that records.

Phase 1 needs a way to send a message and a way to prove one was sent.
Both are here; neither talks to a vendor yet, and that is deliberate —
choosing a provider is a Phase 3 decision and wiring one now would mean
guessing at an API shape and writing a fake for it that matches nothing.

What Phase 1 fixes is the SHAPE, and three rules inside it:

  * A message that cannot be delivered RAISES rather than returning
    quietly. A rent reminder that silently does not go out is worse than
    an error, because the error would have been noticed in a day and the
    silence is noticed at the eviction hearing.
  * Delivery is recorded — to the audit log for real sends, to a list
    for the fake. "We notified them on the 3rd" is a claim that has to
    be evidenced, and a notification with no record is not evidence.
  * NOTHING IS SENT TO A tenant WITHOUT A CONSENT-BEARING REASON. SMS in
    particular is regulated, and an adapter that will send anything to
    anybody is one loop away from a compliance problem.

CONSENT IS NOT MODELLED HERE, and that is the gap this docstring exists
to name. `reason` is recorded, not checked against a stored preference,
because there is no preference table until Phase 3. A caller passing
"marketing" gets it sent and logged as marketing; nothing stops them.
BACKLOG.md carries it.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field

from lib.ops import audit as A

log = logging.getLogger("mf.comms")

# Transactional reasons a message may be sent for. Free text here would
# make the audit trail unqueryable within a year, and the whole point of
# recording a reason is being able to ask "what did we send this person".
REASONS = frozenset({
    "rent_notice", "rent_receipt", "maintenance", "lease", "legal_notice",
    "account", "verification", "owner_report", "vendor_work_order",
})

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s.]+\.[^@\s]+$")
# Deliberately loose. Strict RFC 5322 validation rejects addresses that
# work, and the only real test of an address is a delivery attempt.

E164_RE = re.compile(r"^\+[1-9]\d{7,14}$")


class CommsError(RuntimeError):
    pass


@dataclass
class Message:
    channel: str            # email | sms
    to: str
    subject: str = ""
    body: str = ""
    reason: str = ""
    user_id: int | None = None

    def validate(self) -> None:
        if self.channel not in ("email", "sms"):
            raise CommsError(f"unknown channel {self.channel!r}")
        if self.reason not in REASONS:
            raise CommsError(
                f"unknown reason {self.reason!r}. The vocabulary is closed so "
                f"that 'what did we send this person' stays answerable; add "
                f"a reason deliberately.")
        if not self.body.strip():
            raise CommsError("refusing to send an empty message — it reads "
                             "to the recipient as a system fault and proves "
                             "nothing if produced as a record")
        if self.channel == "email":
            if not EMAIL_RE.match(self.to or ""):
                raise CommsError(f"{self.to!r} is not a usable email address")
            if not self.subject.strip():
                raise CommsError("an email needs a subject; a blank one is "
                                 "filed as spam by most providers")
        else:
            if not E164_RE.match(self.to or ""):
                raise CommsError(
                    f"{self.to!r} is not E.164. A number without a country "
                    f"code is ambiguous, and the ambiguity resolves "
                    f"differently per carrier — sometimes to somebody else's "
                    f"phone.")
            if len(self.body) > 1600:
                raise CommsError("SMS body over 1600 characters would be "
                                 "split into ten-plus segments; say less or "
                                 "send an email")


@dataclass
class FakeComms:
    """Records instead of sending. The default everywhere but production.

    `sent` is the assertion surface: a test proves a rent reminder went
    out by looking here, which is the same question the audit log answers
    in production. Failures can be forced with `fail_on` so the retry
    path is exercised by tests rather than by an outage.
    """
    sent: list = field(default_factory=list)
    fail_on: set = field(default_factory=set)

    def send(self, message: Message, conn=None) -> dict:
        message.validate()
        if message.to in self.fail_on:
            raise CommsError(f"simulated delivery failure to {message.to}")
        record = {"channel": message.channel, "to": message.to,
                  "subject": message.subject, "body": message.body,
                  "reason": message.reason, "user_id": message.user_id}
        self.sent.append(record)
        if conn is not None:
            _audit(conn, message, "fake")
        log.info("[fake] %s to %s (%s)", message.channel, message.to,
                 message.reason)
        return {"ok": True, "provider": "fake", "id": f"fake-{len(self.sent)}"}

    def to_address(self, address: str) -> list:
        return [m for m in self.sent if m["to"] == address]

    def clear(self) -> None:
        self.sent.clear()


def _audit(conn, message: Message, provider: str) -> None:
    """One audit row per delivery. The body is NOT recorded.

    A rent notice quotes an amount and an address; a verification message
    contains a code. mf_audit_log cannot be edited or deleted, so
    anything written there is written permanently — recording that a
    message was sent, to whom, and why is the evidence that is wanted,
    and the body is the part that would turn the log into a second copy
    of everyone's correspondence.
    """
    A.record(conn, action="export", target_type="comms",
             target_id=message.to, actor_user_id=message.user_id,
             actor_label="system",
             detail={"channel": message.channel, "reason": message.reason,
                     "subject": message.subject[:200], "provider": provider,
                     "body_chars": len(message.body)})


class NullComms:
    """Refuses to send anything, loudly. The right thing in a half-built
    deploy, and never the right thing silently.

    from_env() returns this when nothing is configured, so the failure is
    an exception at the send rather than a message nobody ever receives
    and nobody ever misses until it matters.
    """

    def send(self, message: Message, conn=None):
        message.validate()
        raise CommsError(
            f"no comms provider is configured, so this {message.channel} to "
            f"{message.to} was NOT sent. Set MF_COMMS_PROVIDER, or pass a "
            f"FakeComms explicitly if not sending is what you meant.")


def from_env(fake_ok: bool = False):
    """The configured provider. NullComms when there is none.

    `fake_ok` has to be passed deliberately. A fake that stands in for a
    real provider by default is how a month of rent reminders goes
    nowhere while every test passes.
    """
    import os
    provider = (os.getenv("MF_COMMS_PROVIDER") or "").lower()
    if provider == "fake":
        return FakeComms()
    if not provider:
        return FakeComms() if fake_ok else NullComms()
    # Real providers arrive in Phase 3, with their own fakes alongside.
    raise CommsError(
        f"comms provider {provider!r} is not implemented yet. Phase 3 adds "
        f"one; until then the only options are 'fake' and not sending.")

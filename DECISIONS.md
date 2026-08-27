# DECISIONS.md

Assumptions made because the question could not be asked, and choices that would be
expensive to reverse. Newest first. Every entry carries a date.

---

## 2026-08-27 — Auth is standard library, not a dependency
**Assumption.** `hashlib.scrypt` for passwords and hand-written RFC 6238 TOTP, rather
than passlib/argon2/pyotp. Three new packages in the security-critical path of an app
whose other twelve dependencies are pinned and boring is a worse trade than fifteen
lines of HMAC. The TOTP is checked against the published RFC test vectors, so it
interoperates with real authenticator apps rather than only with itself. Reversible:
the hash format is self-describing (`scrypt$n$r$p$salt$hash`), so a future scheme can be
added and old hashes upgraded on next login without a mass reset.

## 2026-08-27 — Portals are separate COOKIES, not a role check
**Decided.** One person can be both a maintenance supervisor and a tenant. Which they
are right now is the door they came through, so each portal has its own cookie name
scoped to `/ops`, and the session records its portal. The browser enforces half of it
(a staff cookie is never sent to a tenant route) and the server enforces the other half
(a tenant token presented under the staff cookie name is still refused). A role check
alone passes the case that matters.

## 2026-08-27 — Session revocation is a privilege epoch, not a session-store scan
**Decided.** `mf_users.privilege_epoch` is copied onto each session at issue and
compared on every request. Any role grant or password change bumps it, killing every
live session on its next request — without scanning the session table, without a cache
to invalidate, and for sessions this process has never seen. `revoke_all_for_user`
still exists for when "there is no live session" is wanted rather than "the next
request is refused".

## 2026-08-27 — `mf_audit_log` immutability is a TRIGGER, not a grant
**Decided, with a known gap.** ARCHITECTURE.md §5.4 asks for an insert-only grant AND a
trigger. Only the trigger is implemented: the app connects as the table's owner on one
`DATABASE_URL`, an owner can re-grant to itself, and a superuser bypasses grants
entirely — so the grant would be a line that looks like a second defence and stops
nothing. It becomes real when the app connects as a separate non-owner role, which is
infrastructure work, and BACKLOG.md carries it as that. The trigger is attacked through
all five paths (UPDATE, DELETE, zero-row UPDATE, zero-row DELETE, TRUNCATE), because
the zero-row cases were genuinely unguarded when first tested.

## 2026-08-27 — Ops migrations fail OPEN at boot, for now
**Assumption, time-limited.** `database.init_db()` applies pending `mf_` migrations and
swallows any failure. That is the opposite of the usual rule and correct only while
there are no ops routes carrying traffic and no ops data at risk: a half-built platform
must never be why `/map` returns 502. It must flip to fail-closed once Phase 2 puts
real data in these tables — serving against an unexpected schema is how data gets
corrupted. BACKLOG.md carries the flip.

## 2026-08-27 — The first administrator is made by a script, not the app
**Decided.** Every repository write needs a Scope, which needs a session, which needs a
user — so the first `platform_admin` cannot be created through the application. The
alternatives were a default account, which never gets deleted, or self-registration on
the staff portal, which is an open door. `scripts/mfops_bootstrap.py` is run once by
whoever already holds `DATABASE_URL` — the credential that would let them write the
rows by hand anyway — and refuses to run again while any administrator exists.

## 2026-08-26 — Host repo is `market-pulse`, not a new service
**Decided by:** owner, mid-session, explicitly ("Do Market Pulse", "do not do accounting").
**Context:** Phase 0 surveyed the two reachable repos. `Accounting`
(`happening-invoice-tracker`) is a live Express + EJS product for disability-services
invoicing on Node's *experimental* `node:sqlite`, with no tests. `market-pulse` is a
FastAPI + Postgres app with 22 test suites and substantial real-estate domain assets.
**Consequence:** the ops platform lives in `market-pulse-v21/` under an `mf_*` schema
namespace with its own money types, its own authorization layer and its own router
package. See ARCHITECTURE.md §1 for what the choice costs.

## 2026-08-26 — Stack is Python/FastAPI, not TypeScript
**Assumption.** CLAUDE.md says "TypeScript strict mode (or the equivalent typed
discipline for the detected stack)". The detected stack is Python 3.11. Resolved as:
type hints on every new ops module, and the repo's existing `check()` harness rather
than pytest, so the ops suites sit alongside the 22 that already exist.

## 2026-08-26 — Authorization via a repository layer, not Postgres RLS
**Assumption, reversible.** CLAUDE.md requires data-layer enforcement and names RLS as
the preferred mechanism. `database.py` opens a per-call connection as a single DB user
with no session context, so RLS would need `SET LOCAL app.current_user` plumbed through
every checkout. Phase 1 instead makes `lib/ops/repository.py` the only path to `mf_*`
tables and adds a test that fails on any route lacking an explicit scope declaration —
so a forgotten guard fails the suite rather than defaulting to open, which is how the
existing `_admin_gate()` fails today. RLS stays in BACKLOG.md as the stronger end state.

## 2026-08-26 — Job queue in Postgres, not Redis
**Assumption.** `SELECT … FOR UPDATE SKIP LOCKED` against an `mf_jobs` table. One fewer
service for a single operator, transactional with the writes that enqueue it, and
adequate well beyond this portfolio's volume.

## 2026-08-26 — Object storage is S3-compatible, not a Railway volume
**Assumption.** Move-in photos decide deposit disputes and lead certificates carry
statutory penalties; both need durability and tamper evidence that a container volume
does not provide. Signed expiring URLs only, SHA-256 recorded at upload.

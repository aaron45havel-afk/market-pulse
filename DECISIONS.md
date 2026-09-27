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

## 2026-09-27 — /multifamily ranks by the user's priorities, not one fixed blend
**Assumption.** The owner asked for a board that filters to their wants ("young
professional", "weather", "low crime") rather than ranking by one formula. Filters are
hard yes/no; five priorities (cash flow, rental yield, 3-yr price growth, neighborhood
income, degree share) are weighted 0–3; four presets set them. Scores are percentiles
among the ZIPs that survived the filters, not the whole state — "best of what you'd
consider". This replaces the old fixed weights, including the fallback that ranked 10%
on `walk_score`, which is a curve on density and not a walk score.

## 2026-09-27 — Area type cut points: 500 and 3,000 people per square mile
**Assumption.** Rural & small town under 500, suburban 500–3,000, urban 3,000+, on
ZIP-average density. 500 is the Census Bureau's urban-fringe threshold; 3,000 is where a
ZIP reads as city blocks. Among the ~8,500 ZIPs the board can underwrite this splits
roughly a quarter / two-fifths / a third. Judgement, not a standard — change the
constants in `zip_finder.AREA_TYPES` if they read wrong.

## 2026-09-27 — Verified-safe rows outrank unverified ones regardless of score
**Assumption.** When the user chooses to show cities with no FBI figure, they appear
after every verified row. Within the verified group rows now sort by score alone; the old
board sorted very-safe before safe regardless of score. The safety tier is already a
filter the user sets, so a second ordering by tier would override their priorities.

## 2026-09-27 — The unverified-safety default stays "Hide"
**Assumption.** Under the default Safe tier only 3 Ohio ZIPs survive, because 241 of the
270 that fit the budget are in cities with no FBI figure. Defaulting to "Show, flagged"
would make the board look far more useful and would put unmeasured places on a page
that says "where you would live". Left fail-closed; the funnel now says exactly how many
the gate removed for lack of data, and the fix is more crime coverage, not a looser default.

## 2026-09-27 — Zillow's 12-month forecast is not offered as a filter or priority
**Assumption.** Price growth uses the ZIP's own measured 36-month trend. The forecast
column is 100% populated but is a model output; offering a measured and a modelled growth
signal side by side invites double-counting. Can be added later as its own labelled filter.

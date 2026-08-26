# DECISIONS.md

Assumptions made because the question could not be asked, and choices that would be
expensive to reverse. Newest first. Every entry carries a date.

---

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

# Project: Multifamily Operations Platform

## What this is
An operations platform for a multifamily property owner-operator running buildings in
San Leandro, California and in Rhode Island. It handles the full lifecycle: tenant
onboarding and offboarding, rent collection, rent increases, regulatory compliance,
preventive maintenance, vendor management, operating costs, insurance and debt tracking,
an owner/client portal, a tenant portal, and rental listing syndication.

## Operating footprint
- **San Leandro, Alameda County, California** — timezone America/Los_Angeles
- **Rhode Island** (statewide; municipal rules vary) — timezone America/New_York

These are two genuinely different legal regimes. Nothing jurisdiction-specific may be
hardcoded. See "Jurisdiction rules" below.

## Non-negotiable engineering rules

### Money
- All monetary amounts are stored as **integer minor units (cents)** with an explicit
  ISO-4217 currency column. Never `float`, never `double`, never JS `number` for money.
- All monetary math happens server-side in integer arithmetic. Formatting to a decimal
  string happens only at the presentation edge.
- The rent ledger is **append-only**. Corrections are reversing entries, never updates
  or deletes. Every entry carries `created_by`, `created_at`, and a `reason`.
- Every entry that moves money references an `idempotency_key`. Re-processing the same
  payment webhook twice must be a no-op.

### Dates and deadlines
- Store timestamps as UTC `timestamptz`. Store *legal dates* (notice served, lease start,
  rent due) as `date` in the **property's local timezone**, computed from the property's
  jurisdiction, never from the server's timezone or the browser's.
- Any deadline calculation (notice periods, deposit return windows, inspection due dates)
  must be a pure, unit-tested function taking `(jurisdiction, event_date, rule)`.
  Off-by-one errors here are legal exposure, not cosmetic bugs.

### Jurisdiction rules
- Jurisdiction-specific rules live in **data**, not in code branches. There is a
  `jurisdictions` table and a `jurisdiction_rules` table. A rule row carries:
  `rule_key`, `value` (jsonb), `effective_from`, `effective_to`, `authority`,
  `source_url`, `source_citation`, `last_verified_at`, `verified_by`.
- Code asks "what is the value of rule X for jurisdiction Y on date Z". Code never
  contains `if (state === 'CA')` for a legal rule.
- Any rule older than 180 days by `last_verified_at` renders with a staleness warning
  and is surfaced on the compliance dashboard for re-verification.
- The platform never asserts a legal conclusion. It surfaces a tracked requirement, its
  cited source, and its verification date, and requires a named human to sign off before
  any tenant-facing legal notice is generated or sent.

### Multi-tenancy and access
- Roles: `platform_admin`, `division_manager`, `staff` (scoped by division and property),
  `owner_client` (read-only, scoped to owned entities), `tenant` (scoped to own lease),
  `vendor` (scoped to assigned work orders and own compliance documents).
- Authorization is enforced at the **data layer** (row-level security or an equivalent
  server-side scoping layer), not only in UI routing. A tenant hitting an API route
  directly must not see another tenant's ledger.
- Every read of tenant PII, and every write anywhere, lands in an immutable `audit_log`
  with actor, action, target, timestamp, and IP.

### Integrations
- Every third-party integration goes behind an adapter interface in `lib/integrations/`
  with: a typed client, a fake/in-memory implementation for tests, retry with exponential
  backoff and jitter, circuit breaking, and structured logging of every outbound call.
- No integration is on the critical path of a page render. External calls are queued jobs.
- Credentials come from environment variables only. If you ever find a secret in the
  repo, stop and tell me.

### General
- TypeScript strict mode (or the equivalent typed discipline for the detected stack).
- Every business rule gets a unit test. Every money path and every deadline calculation
  gets a unit test with explicit edge cases (leap years, DST transitions, month-end).
- Prefer boring, well-supported libraries over clever ones.
- When a requirement is ambiguous, **ask me** rather than guessing. When you make an
  assumption because you couldn't ask, write it into `DECISIONS.md` with a date.

## Working agreement
- Work only on the phase I give you. If you spot something out of scope, add it to
  `BACKLOG.md` and keep going.
- Before writing code, restate the plan in five bullets and list files you'll touch.
- After finishing, output: files changed, migrations added, env vars needed, what I
  should manually test, and what you deliberately left out.
- Do not mark work complete if tests fail or you stubbed something. Say what's stubbed.

---

# Host-repo addendum (written by Phase 0 — read with STACK.md)

This platform is being built **inside the existing `market-pulse` repo**, per the
owner's decision on 2026-08-26. That repo already contains a large, live FastAPI
application. Three consequences that override the defaults above:

1. **The stack is Python/FastAPI, not TypeScript.** "TypeScript strict mode" resolves to
   the typed-discipline clause: type hints on every new module, and the repo's existing
   `check()` test harness (`tests/test_*.py`, exit 0 = pass) rather than pytest.

2. **The existing app violates two of the non-negotiables and is not being retrofitted.**
   Market-pulse stores money as `DOUBLE PRECISION` and enforces authorization in route
   handlers, not at the data layer. Those are acceptable for an analysis tool reading
   public filings. They are **not** acceptable for tenant money and legal notices. The
   ops platform therefore lives in its own schema namespace (`mf_*` tables) with its own
   rules, and there is a hard boundary between the two. See ARCHITECTURE.md.

3. **`market-pulse-v21/` is load-bearing and in production.** Nothing in this build may
   change existing behaviour on `/schloss`, `/hundred`, `/moats`, `/holt`, `/map`,
   `/compounders`, `/lynch`, `/catalysts`, or the ZIP boards. Adding to `main.py`,
   `database.py`, `base.html` nav, and `tests/` is expected; editing existing analysis
   modules is not.

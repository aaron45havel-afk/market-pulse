# STACK.md — what this repo actually is

Written by Phase 0 on **2026-08-26**. Everything here is observed from the working tree,
with file paths as evidence. Nothing is inferred from what a repo of this shape usually
contains.

---

## 1. Repo layout

The repo holds **two unrelated deployables** plus loose documents.

| path | what it is | deploy |
|---|---|---|
| `market-pulse-v21/` | The application. FastAPI + Jinja2, ~46,000 lines of Python. | Railway |
| `focusedops-site/` | A single static marketing page for a consultancy, "FocusedOps — Custom software for contract-heavy finance teams". `index.html` (13KB) + `vercel.json`. No forms, no backend, no build step. | Vercel |
| `TODO.md`, `Deal_Comparison_Matrix.xlsx` | Loose working files at root. | — |

**Answer to Phase 0's question ("marketing site, web app, or marketing site with an app
bolted on?")**: it is a **web app with an unrelated static one-pager sharing the repo**.
They share no code, no auth, no database, and no deploy target. `focusedops-site` is not
the property business's website and is not load-bearing for this build.

## 2. The application — `market-pulse-v21/`

### Language and runtime
- Python 3.11 (`.github/workflows/*.yml` all pin `python-version: '3.11'`)
- No `pyproject.toml`, no lockfile. Dependencies pinned in `requirements.txt`.

### Dependencies (`market-pulse-v21/requirements.txt`, verbatim)
```
fastapi==0.115.6
uvicorn[standard]==0.34.0
jinja2==3.1.4
pandas==2.2.3
fredapi==0.5.2
apscheduler==3.10.4
python-dotenv==1.0.1
psycopg2-binary==2.9.10
python-multipart==0.0.20
PyMuPDF>=1.24.0
```
Ten direct dependencies, all current and maintained. `PyMuPDF` is the only unpinned one
(`>=`), which is a reproducibility gap worth closing. No deprecated packages, no
advisories observed. Notably **absent**: any ORM, any migration tool, any test runner,
any object-storage client, any queue library.

### Framework and rendering
- FastAPI, server-rendered Jinja2 templates. No SPA, no client build step.
- `main.py` is a single ~5,900-line module holding every route.
- 34 templates in `market-pulse-v21/templates/`, all extending `base.html`.
- Sidebar navigation is hand-maintained inside `templates/base.html`.

### Data stores — there are two, and they are used differently
1. **PostgreSQL** via raw `psycopg2` (`market-pulse-v21/database.py`, ~2,000 lines).
   - **No ORM and no migration framework.** Schema is created by idempotent
     `_ensure_*_tables()` functions issuing `CREATE TABLE IF NOT EXISTS` and
     `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`, called from `init_db()` at boot.
   - Existing table groups: `landscaper_*`, `hh_*` (household), `hundred_hand`,
     `pm_*` (permit moats), `user_prices`, `price_history`, `portfolio_*`, `users`,
     `crm_contacts`.
   - Connections are per-call, not pooled, with a retry/backoff wrapper for Railway's
     Postgres readiness (`_get_conn`, `database.py:20`).
2. **SQLite** — `market-pulse-v21/data/zips.db`, 27MB, committed to git. One `zips`
   table, 25,774 rows, read-only at runtime. Rebuilt monthly by a GitHub Action.

Plus ~20 JSON files in `market-pulse-v21/data/` that are build artefacts committed to
the repo (`schloss.json` 6.9MB, `compounders.json` 1.5MB, etc).

### How money is currently stored — **this matters**
`DOUBLE PRECISION` and `NUMERIC`. Evidence in `database.py`:
- `:248` `value DOUBLE PRECISION` (hand-entered criteria)
- `:371-372` `market_cap_at_add DOUBLE PRECISION`, `anchor_price DOUBLE PRECISION`
- `:409` `target_price DOUBLE PRECISION`
- `:216` `ALTER TABLE hh_budget_items ADD COLUMN IF NOT EXISTS planned NUMERIC`

This directly violates CLAUDE.md's first non-negotiable. It is defensible for the
existing app — those are analysis figures derived from public filings, where a float is
the honest representation of an estimate. It is **not** defensible for a rent ledger.
See ARCHITECTURE.md for the boundary.

### Auth — what exists today
`market-pulse-v21/auth.py` (~170 lines) plus helpers in `main.py`:
- HMAC-signed session cookie `mp_session` carrying `{email, role}`, 30-day TTL
  (`auth.py:32-57`)
- Google OAuth login flow (`auth.py:94-160`)
- A shared-secret `ADMIN_TOKEN` accepted via query param, `X-Admin-Token` header, or an
  `mp_admin` cookie (`main.py:_check_admin_token`, ~`:4469`)
- Role gating is **per-route**, via `_admin_gate(request)` returning a 401 JSONResponse
  (`main.py:~4496`), called at the top of each protected handler.

**There is no row-level security, no repository pattern, and no data-layer scoping.**
Every route that forgets to call `_admin_gate` is open. This is the single largest gap
between the current codebase and CLAUDE.md's access-control requirement, and it is
Phase 1 work, not an assumption to carry forward.

**No MFA.** No session rotation on privilege change. No revocation path beyond cookie
expiry.

### Background work
- **No in-process job queue.** `apscheduler` is in `requirements.txt` but the heavy
  lifting is done by **25 GitHub Actions workflows** in `.github/workflows/` that fetch
  data on cron, rebuild the JSON/SQLite artefacts, and commit them back to `main`.
- That pattern works well for monthly public-data refreshes. It is unsuitable for
  payment webhooks, notice delivery, or anything with a per-tenant SLA.

### File and document storage
**None.** No S3/boto3, no blob client, no signed-URL helper. Uploads today are handled
by `python-multipart` into request memory. Phase 3's move-in photo capture and Phase 7's
evidence documents have no home in the current stack.

### Tests
- A hand-rolled harness, not a framework. 22 files at `market-pulse-v21/tests/test_*.py`.
- Each is a standalone script: a `check(cond, msg)` counter, a `_FAILS` list, and
  `sys.exit(0|1)`. Run with `python tests/test_x.py`.
- Coverage is genuinely good for domain logic (`test_schloss.py` 225 checks,
  `test_moats.py` 135, `test_holt.py` 75, `test_rent_ladder.py` 51) and effectively zero
  for routes, auth, and templates.
- CI runs the suites inside the data-refresh workflows before spending API calls; there
  is no standalone test-on-push workflow.

### Deploy and configuration
- Railway, from `main`. Auto-deploy on merge.
- Config by environment variable throughout: `DATABASE_URL`, `ADMIN_TOKEN`,
  `CENSUS_API_KEY`, `FRED_API_KEY`, `CRON_SECRET`, `HUD_API_TOKEN`, Google OAuth pair.
- No secrets found in the working tree.

### Styling
Hand-written CSS in `templates/base.html` with a design-token block (`--ink`, `--surface`,
`--primary`, density and dark-theme variants). No Tailwind, no component library. Chart.js
via CDN. Leaflet via CDN on the map pages.

---

## 3. What in here is load-bearing

In production and must not break:

- `main.py` route table and every template it renders. Live boards: `/schloss`,
  `/hundred`, `/moats`, `/holt`, `/compounders`, `/lynch`, `/catalysts`, `/quiet-value`,
  `/aristocrats`, `/global-values`, `/map`, `/multifamily`, `/househack`, `/headroom`,
  `/value-add`, `/real-mortgage-index`, `/fair-value`, `/pipeline`.
- `database.py` — the `pm_*` tables carry hand-entered permit-moat data that exists
  nowhere else and cannot be rebuilt.
- `data/zips.db` and the committed JSON artefacts — regenerating them costs a full
  Actions run against rate-limited APIs.
- The 25 GitHub Actions workflows and their cron schedules.
- `templates/base.html` — every page extends it. Nav edits are additive and safe; token
  or layout edits are not.

---

## 4. Assets already here that this build should use rather than rebuild

This is the strongest argument for the owner's choice of host repo.

| asset | where | why it matters |
|---|---|---|
| `zips.db` — 25,774 US ZIPs with demographics, income, home values | `data/zips.db` | Market context for both operating footprints |
| **Rent ladder** — ZORI / HUD SAFMR / HUD FMR / Census ACS with per-tier provenance | `rent_ladder.py`, `scripts/refresh_rents.py` | Phase 5 needs a "market comp delta" and Phase 12 needs a listing rent. This is exactly that, already built and tested, with the HUD token already provisioned. |
| Property analysis: house-hack, value-add, multifamily scenarios, headroom | `househack.py`, `value_add.py`, `headroom.py` | Phase 14's acquisition pipeline underwriting |
| FHA/PITI, property tax and insurance tables by state | `data_providers.py`, `main.py:_fha_piti` | Phase 10 debt modelling |
| Crime, climate and hazard data by region | `data/regions_*_{climate,hazard}.json` | Phase 10 insurance (RI flood, CA earthquake) |
| Postgres migration idiom that works on Railway | `database.py:_ensure_*_tables` | Phase 1 schema can follow a proven pattern |
| The `check()` test harness | `tests/` | Phase 1 tests have an established form |

---

## 5. Gaps between this codebase and CLAUDE.md, stated plainly

These are not reasons to change host. They are Phase 1 scope that must not be assumed
away.

| CLAUDE.md requirement | current state | severity |
|---|---|---|
| Money in integer cents | `DOUBLE PRECISION` / `NUMERIC` | **Blocking** for the ledger. New schema only; existing columns stay. |
| Authorization at the data layer | per-route `_admin_gate()` calls | **Blocking.** A missed call is an open route. Needs a scoping layer or RLS. |
| Immutable `audit_log` | none | **Blocking** |
| MFA for admin roles | none | **Blocking** for Phase 1 acceptance |
| Queued jobs, never on render path | GitHub Actions cron only | **Blocking** for Phase 4 webhooks |
| Object storage with signed expiring URLs | none | **Blocking** for Phase 3 photos |
| Migrations that run up *and* down | `CREATE TABLE IF NOT EXISTS`, no down path | High — Phase 1 acceptance names it |
| Typed discipline | type hints present in newer modules, absent in older | Medium |
| Integration adapters with fakes | partially — `rent_ladder`/`refresh_rents` follow it | Medium; the pattern exists to copy |
| Transactional email/SMS behind an adapter | none | Medium |
| Structured logging with request IDs | plain `logging`, no request IDs | Medium |
| `TimeService` / `JurisdictionClock` | code calls `datetime.now()` directly | High — legal dates depend on it |

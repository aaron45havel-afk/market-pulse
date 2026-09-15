# BACKLOG.md

Out-of-scope items spotted while working a phase. Add, keep going, do not detour.

Format: `- [PHASE-SEEN] item — why it matters`

---

## Open

- [P0] Postgres row-level security for `mf_*` tables — ARCHITECTURE.md §2 uses a
  repository layer plus a route-enumeration test instead, because the app connects as a
  single DB user with per-call connections and no session context. RLS is the stronger
  guarantee and should replace it once connection handling supports `SET LOCAL`.
- [P0] `PyMuPDF>=1.24.0` is the only unpinned dependency in `requirements.txt` —
  reproducibility gap.
- [P0] No standalone test-on-push CI workflow. The 22 suites run only inside the
  data-refresh Actions, so a PR touching domain logic is not gated on them.
- [P0] `market-pulse-v21/main.py` is ~5,900 lines. Existing routes are out of scope for
  this build, but the file is past the point of comfortable review.
- [P0] `focusedops-site/` is an unrelated static one-pager sharing this repo. Not
  load-bearing here; worth splitting out eventually.
- [P0] The existing analysis app stores money as `DOUBLE PRECISION` and gates access
  per-route. Acceptable for public-filing analysis, out of scope to retrofit, but it
  means two conventions live in one process — see ARCHITECTURE.md §2.
- [P0] `refresh_compounders.py` writes a wrong `price` for some tickers (Booking
  Holdings at 0.8x P/FCF, Trade Desk at $13.80). Guarded on `/holt`, unfixed at source,
  and every board reading `pfcf_now` is affected.
- [P0] `CRON_SECRET` was exposed in an earlier session and has not been rotated.
- [P1] `mf_audit_log` has the immutability trigger ARCHITECTURE.md §5.4 asks for but not
  the insert-only grant. A grant is meaningless while the app connects as the table's
  owner on a single `DATABASE_URL` — the owner can re-grant to itself and a superuser
  bypasses grants entirely. Needs a separate non-owner application role, which is
  infrastructure work, not a migration. The trigger is the whole guarantee until then.
- [P1] Ops migrations fail OPEN at boot (`lib/ops/bootstrap.py`): a failure is logged and
  the app keeps serving. Correct while no ops routes exist and no ops data is at risk;
  it must flip to fail-closed the moment Phase 1-D puts real traffic on `mf_*`, because
  serving against an unexpected schema is how data gets corrupted.
- [P1] `JurisdictionClock.deadline(counting="business")` counts weekdays only — public
  holidays are not modelled. Phase 7 needs real per-jurisdiction holiday calendars
  before any business-day deadline is treated as legally reliable.
- [P1] **Comms consent is recorded, not checked.** `integrations/comms.py` takes a
  `reason` from a closed vocabulary and writes it to the audit log, but nothing compares
  it against a stored preference — there is no preference table until Phase 3. SMS in
  particular is regulated. A caller passing a marketing reason today gets it sent.
- [P1] No document scoping. `mf_documents` is readable only by `platform_admin`, because
  its `visibility` column would show every tenant every tenant-visible document in the
  organization. Phase 2's `mf_leases` gives it a real owner to scope by; until then the
  grant is deliberately absent from every other role.
- [P1] `mf_user_roles.property_id` has no foreign key — `mf_properties` does not exist
  yet. Phase 2's migration must add the constraint, not just the table.
- [P1] Object storage falls back to local disk when `MF_S3_BUCKET` is unset, with a
  warning. On Railway that is an ephemeral volume, which is exactly the durability
  failure ARCHITECTURE.md §4 rules out for documents that decide deposit disputes.
  Configure R2 or B2 before any real document is uploaded.
- [P1] `jobs.run_forever` has no test — a loop that never returns cannot have one. All
  the logic is in `run_one`, which is tested; the shell is kept thin for that reason,
  and it is still untested code running as a production process.
- [FCFQ] The IFRS debt tag ladder in `refresh_compounders.BALANCE_TAGS` is too narrow.
  Foreign filers whose borrowings sit under element names it does not carry come through
  with a fraction of their real debt — Korea Electric, Ecopetrol, Toyota, POSCO, Takeda
  and Wipro all did. `fcf_quality.debt_cross_check` refuses those rows rather than ranking
  them, so nothing wrong reaches the board, but the fix is more `ifrs-full` borrowing tags,
  not more guards. 48 rows are currently refused this way.
- [FCFQ] `inferred_zero_fault` cannot tell a genuinely debt-free large cap from an unmapped
  debt tag, so it refuses both above $10bn. Vertex, Intuitive Surgical, Shopify and Datadog
  are really debt-light and are really excluded — 34 rows. Widening the tag ladder is what
  shrinks this; raising the threshold alone would let General Motors back in at a market-cap
  denominator and an inflated yield.
- [FCFQ] `snapshot_screens` declares its inputs by CONTENT HASH, so it can see that one
  moved but never that one rolled BACKWARD — `freshness.verdict`'s fault branch is
  unreachable for it. None of its three inputs offers a usable date: `zips.db` is SQLite
  with no metadata, `norcal_condo.json` writes a bare `"2026-07"` that `fromisoformat`
  rejects, and `headroom/crime.json` is annual. Giving `refresh_norcal` and
  `build_national_zips` an ISO `_meta.as_of` each would upgrade those two stamps to dated
  ones and turn the fault branch on; the guard needs no change.
- [FCFQ] The freshness guard covers the two builds that JOIN (`fcf-quality`,
  `screen-history`). It is not applicable to the fetchers, whose inputs are the network —
  those need a delta guard on the result size instead, which `refresh_lynch_screener` and
  `refresh_hundred` already carry and `refresh_screener`, `refresh_quiet_value`,
  `refresh_catalysts` and `refresh_aristocrats` do not. A fetcher that comes back with an
  empty or halved result currently publishes it.

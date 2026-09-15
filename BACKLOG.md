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

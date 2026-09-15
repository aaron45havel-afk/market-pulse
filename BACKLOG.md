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
- [FCFQ] `refresh-fcf-quality` can run against a stale `compounders.json` if both are
  dispatched by hand: the compounders job takes ~36 minutes and the FCF build takes 13
  seconds, so a manual run of both in sequence produces a snapshot from the PREVIOUS
  month's inputs. It happened on the first real run. A `workflow_run` trigger chained to
  the compounders workflow would remove the race.

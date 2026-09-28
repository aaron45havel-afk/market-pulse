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
- [P0] `market-pulse-v21/main.py` is ~5,900 lines. Existing routes are out of scope for
  this build, but the file is past the point of comfortable review.
- [P0] `focusedops-site/` is an unrelated static one-pager sharing this repo. Not
  load-bearing here; worth splitting out eventually.
- [P0] The existing analysis app stores money as `DOUBLE PRECISION` and gates access
  per-route. Acceptable for public-filing analysis, out of scope to retrofit, but it
  means two conventions live in one process — see ARCHITECTURE.md §2.
- [P2] Compounders share counts are split-restated for domestic filers only. A foreign
  filer's ADR splits and ratio changes aren't its ordinary shares' splits, so its counts
  stay as filed and a split still reads as dilution in `shares_cagr5` (Toyota's 5-for-1
  in 2021 shows as +73% a year). The filer's own later 20-F comparatives, which restate
  prior years, would give the split factor without Yahoo.
- [P2] When Yahoo has no chart for a name, its `shares_cagr5` stays as filed (no split
  history to restate it with). The row already has no valuation term in that case.
- [P2] `fix_share_scale` needs a break to anchor on. A company whose WHOLE share series
  is in the wrong unit (Nutanix reads 0.0x P/FCF) is left as filed; the `/holt` and
  compounders guards refuse it. The cover-page count (`dei:EntityCommonStockSharesOutstanding`)
  would be an anchor for those.
- [P0] `CRON_SECRET` was exposed in an earlier session and has not been rotated.
- [P1] `mf_audit_log` has the immutability trigger ARCHITECTURE.md §5.4 asks for but not
  the insert-only grant. A grant is meaningless while the app connects as the table's
  owner on a single `DATABASE_URL` — the owner can re-grant to itself and a superuser
  bypasses grants entirely. Needs a separate non-owner application role, which is
  infrastructure work, not a migration. The trigger is the whole guarantee until then.
- [P1] The ops fail-closed gate is on `deps.db()`, so the login PAGES still render when
  the schema is unverified and only 503 on submit. Harmless — the POST is gated — but a
  form that accepts a password it cannot check is a worse error message than a page
  saying the platform is down. Wants a readiness check on the GET handlers, or a banner.
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
- [MF-CENSUS] The 2024 ACS suppresses income for small ZCTAs, and the ZIP build requires
  income, so the first keyless rebuild skipped 486 Zillow ZIPs (median population ~485;
  20 with 1,500+). Keeping them with a null income would need `crime_proxy` and the
  composites to tolerate it.
- [MF-CRIME] At the defaults Ohio's safety gate still removes 401 ZIPs for no FBI figure.
  Mostly ZIPs whose postal city isn't a police jurisdiction (townships, unincorporated
  areas policed by a sheriff) and the 1,067 matched agencies with no complete year.
  A sheriff's figure covers the whole unincorporated county, so using it would need its
  own rule and label rather than being treated as a city rate.
- [MF-CRIME] Rates are city-wide. A ZIP-level signal (e.g. agency incident locations, or
  the Census tract of each NIBRS incident where published) would separate a big city's
  safe and rough neighborhoods; nothing free publishes it nationally today.
- [MF-CRIME] 27 researcher-flagged cities stay unverified because their FBI figure is
  under 100 per 100k — a human pass could clear the genuinely safe ones.
- [MF-HAZARDS] FEMA's inland-flood model (IFLD) sets a baseline nearly everywhere, so
  downtown Phoenix ($148/yr per $100k) comes out above Miami Beach ($88) and levee-protected
  New Orleans below the national median. The page says so. A second flood source — NFIP
  claims per policy by ZIP (OpenFEMA), or FEMA flood-zone share of each ZIP — would let the
  board show where the model and history disagree.
- [MF-HAZARDS] Hazard dollars are per $100k of building value, and the purchase price
  includes land. A land-share estimate per ZIP (e.g. from the NRI's own BUILDVALUE against
  home values) would let the page state the yearly figure on the user's actual building
  instead of an upper bound.
- [MF-WEATHER] No elevation correction: a ZIP matched to a station a few hundred metres
  lower reads warm by roughly 2°F per 300 m. The station's elevation is in
  `zip_climate.json`; the ZIP's is not in `zips.db`.
- [MF-RENTS] Outside New England, ZIPs still join HUD counties by name, and a handful of
  Alaska ZIPs miss on names that differ in substance ("Anchorage Borough" vs HUD's
  "Anchorage Municipality"). HUD's ZIP-to-county crosswalk (`/usps?type=2`, the same API
  the New England town join uses as type 11) would replace the name join with codes.
- [MF-RENTS] A New England ZIP split roughly evenly between two towns in different HUD
  rent areas gets no FMR (it falls to ACS). Weighting the two areas' FMRs by the
  crosswalk's residential shares would give it one, at the cost of a figure HUD never
  published.
- [MF-RENTS] One New England town the crosswalk places a ZIP in came back from HUD with no
  rent, so 05452 (Essex, VT) lost its FMR; it keeps its Zillow rent. Probably a town newer
  than HUD's town list (Essex Junction became a city in 2022) — not verified. Asking HUD
  for crosswalk towns its list doesn't name would show whether it knows the new code.
- [MF-RENTS] `rent_ladder.spread()` reads ZORI above SAFMR as a "tight market" on the
  premise that SAFMR is a floor. The first national run showed SAFMR at 1.01× ZORI at the
  median, so about half of ZIPs would read "soft". Nothing displays the reading today;
  re-derive the cut points from the measured distribution before anything does.
- [MF-RENTS] The monthly rent refresh now makes ~3,200 HUD requests (~70 min). HUD's
  statedata endpoint or the published SAFMR/FMR files could cut that to ~50 requests.
- [MF-RENTS] HUD's FMR year (FY2027) is parsed but not stored; `rent_as_of` is the run
  date. Storing the fiscal year would let the page say which year's HUD rents it shows.
- [MF-FINDER] The scenario card prints negative dollar figures as `$-5,543` (per year,
  and "if you move out") — pre-existing, the sign belongs before the dollar sign.

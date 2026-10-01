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
- [P2] Compounders' net debt/EBIT (the debt GATE) still reads debt through `_annual_series`,
  which keys balance-sheet instants on the filing's fiscal year — a 10-K's prior-year
  comparative competes for the slot — and can add `LongTermDebt` (a total) to `DebtCurrent`
  (AT&T: ~$144bn vs ~$136bn). Both err toward stricter. `balance_sheet()` now does this
  properly for enterprise value; moving the gate onto it would need the FCF-quality
  cross-check, which compares the two paths, to be rethought.
- [P2] Capex filed under company-specific tags (ConocoPhillips since 2023, NextEra) is not
  read, so those rows have no free cash flow. Only us-gaap tags are on the list.
- [P2] The EBIT fill (pre-tax income + interest) splices into an operating-income series
  for the years it covers; for a company with large non-operating items it will differ
  from the operating line it replaces.
- [P2] Debt filed only under a company's own taxonomy (Deere, Ford, NVR, Vale) stays
  unknown on compounders and so off the FCF-quality board. `LineOfCredit` is left unread
  on purpose — Abercrombie tags its undrawn $500m facility under it — as is
  `DebtInstrumentCarryingAmount`, which is often one instrument, not the total.
- [P2] A measured debt figure smaller than the year's interest bill is still published: Ball
  ($2m measured, $314m of interest — its notes are under a tag not read), Telefónica,
  Televisa, Carriage Services' revolver. About 20 rows. The same interest-vs-revenue evidence
  that now guards a zero could guard these, but IFRS `FinanceCosts` runs well above debt
  interest (Ambev's reads 130% of its debt), so it needs a narrower interest tag first.
- [P2] Lynch: when `NetIncomeLoss` stops being filed but EPS continues (Bloom Energy after
  2023, Estée Lauder after 2020), the units check compares this year's EPS with an older
  year's net income and rejects the row as `units_unverified`. The P/E would have been
  built on that stale figure too. A net-income fallback tag
  (`NetIncomeLossAvailableToCommonStockholdersBasic`) would recover them — and checking the
  units identity against income available to common, not `NetIncomeLoss`, would stop
  preferred dividends failing it (Occidental, HPE; newly visible once screened under their
  common tickers, as are Ford and Dow).
- [P2] Lynch has no staleness gate on the EPS series itself: Cheetah Mobile's and Solaris'
  last EPS is 2018, Riskified's 2022, while their net income is current. `dormant` looks
  at revenue/net income/assets only.
- [P2] Lynch and the 100-bagger read US-GAAP tags only. IFRS filers (TotalEnergies, Spotify,
  AstraZeneca, UBS — ~340 on Lynch, ~470 on the 100-bagger) are now labelled "files under
  IFRS, not read" instead of "equity not filed"/"dormant", but they are still not measured.
  The compounders build already reads `ifrs-full` for revenue, debt and cash; the same
  ladder would bring them onto both boards.
- [P2] 100-bagger banks: sales growth, gross margin and return on capital are undefined for
  them in the tags read, so most banks land in could-not-measure. A bank revenue measure
  (net interest income + noninterest income) would restore criterion 1; the other two have
  no bank equivalent and should say so per criterion.
- [P2] Lynch reads no partnership equity (`PartnersCapital`), so MLPs such as Alliance
  Resource Partners are `no_equity` (could-not-measure). One tag on the equity ladder.
- [P2] A fund that companyfacts answers with a 404 (Nuveen NAD, NVG) still reads as
  `facts_unavailable`, whose label now says "a failed request, or no XBRL filings at all".
  Telling the two apart needs the HTTP status carried out of `_get`.
- [P2] Lynch: an EPS rounded to the cent on a tiny figure (-0.01 against a true -0.006)
  fails the 35% units identity (YSG). Harmless — a one-cent EPS is not a Lynch candidate —
  but it is counted as a units problem.
- [P2] Aristocrats: ten names have no SEC company statements (OTC ADRs — Nestlé, Roche, Kao,
  Spirax, Halma, Bunzl, Metro, Canadian Utilities — plus SJW and Lancaster Colony, which the SEC
  ticker map did not match), so their safety rests on a seed or stays unmeasured. Leverage is
  on GAAP EBITDA; a company with large one-offs in all three years (BDX, SWK) reads higher
  than on the adjusted figure analysts quote.
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
- [FCFQ] IFRS filers that tag borrowings only by instrument — notes and debentures, bonds,
  secured bank loans (`NotesAndDebenturesIssued`, `NoncurrentPortionOfNoncurrentBondsIssued`,
  `…SecuredBankLoansReceived`) — are read at a fraction of their debt or not at all:
  Telefónica, Televisa, SK Telecom, Grifols. Summing those parts would fix them; they are
  disjoint by instrument, but a filer that also tags `LoansReceived` would double count.
  `fcf_quality.debt_cross_check` now refuses 4 rows (was 48 before the tag-reading fixes).
- [FCFQ] `inferred_zero_fault` refuses an inferred debt-free company above $10bn: 10 rows
  (was 34), among them Intuitive Surgical, Garmin and Logitech, which really are debt-free.
  A company that last REPORTED zero debt is no longer flagged as inferred, which is what
  cleared Vertex, Shopify and the rest; the ten left never tagged debt at all.
- [P2] A combined debt tag that leaves something out stands over parts that would catch it:
  Home Depot's `DebtLongtermAndShorttermCombinedAmount` ($51.3bn) omits $4.5bn of commercial
  paper. The rule trusts filer totals because summing parts doubles filers who tag one
  amount twice (Lumentum, Cenovus); telling the two apart needs the parts to be checked for
  duplicates, not a bigger threshold.
- [P2] `us-gaap:CashEquivalentsAtCarryingValue` (Hovnanian, Inter Parfums) is cash
  equivalents only, so it is not read as cash; those rows have no enterprise value. Paccar
  files no undimensioned cash total at all.
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
- [FCFQ-PRICE-DATE] FCF Quality divides the compounders refresh's cash flow (2026-09-28) by
  the Schloss file's market cap (2026-09-07). The page prints both dates, but a price three
  weeks older than the cash flow can move a yield, and the P/FCF fault test reads that same
  market cap. Refresh Schloss before the FCF build, or carry market cap in compounders.json.
- [COMP-DRIFT-MEDIAN] Compounders' valuation median spans 7 years including 2020-21. Storing per-year
  P/FCF in compounders.json would let the median exclude the peak and be re-measured offline.
- [MAP-REBUILD] Phase 2: Redfin ZIP market activity (DOM, months of supply, sale-to-list, price drops,
  homes sold; 90-day windows by property type) into zip_profile.db, dated, with a staleness rule — the
  ZIP file ended 2026-05-31; check whether the metro/county trackers are fresher. Phase 3: the new ZIP
  page on zip_profile.db (editable underwriting card, owner card, trends from zip_series.json.gz with
  real dates, FEMA/NOAA risk, Census margins). Phase 4: the new map (one metric at a time, filters,
  table, no composites). Phase 5: retire composites/forecast/proxy columns from zips.db and their readers.
- [ZIP-PROFILE-RISK] FEMA hazards and NOAA climate cover the ~25.7k zips.db ZIPs only; extend
  refresh_zip_hazards/refresh_zip_climate to all 32.8k ZCTAs so Census-only ZIPs get risk figures.
- [ZIP-PROFILE-SIZE] zip_profile.db is 20.3 MB and zip_series.json.gz 6.8 MB, committed monthly. Store the
  repeated basis strings as codes, and consider a release asset or data branch for the series.
- [ZIP-PROFILE-CITY] ZIPs Zillow does not cover have no city name (county only); a USPS/HUD ZIP-city
  crosswalk would name them.
- [CRIME-JOIN] FBI crime is joined by Zillow mailing-city name; before it appears on the new pages, weight
  it by the Census ZCTA-to-place file and show it as a police-jurisdiction figure, never a ZIP's.
- [REDFIN-FROZEN] Redfin's public market trackers stopped updating after 2026-06-02 (data through
  2026-05-31). The old map's state layers (days on market, sale-to-list, price drops, homes sold, months of
  supply) read refresh_redfin.py's state file and present May data as current. Switch them to Realtor.com
  state/metro core metrics, or label the month. If Redfin resumes, the ZIP profile picks it up unchanged.

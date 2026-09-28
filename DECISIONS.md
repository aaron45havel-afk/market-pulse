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

## 2026-09-27 — Weather is the nearest station that measures it, within 40 km
**Assumption.** Each ZIP takes NOAA's 1991–2020 normals from the nearest station that
measures the thing asked about — temperature and snow are matched separately, because
most stations measure only precipitation. "Winter average low" is the Dec–Feb mean daily
minimum and "summer average high" the Jun–Aug mean daily maximum. Stations whose normals
NOAA flagged "E" (estimated from neighbours) are skipped: an estimate beside a measured
station would win on distance alone. Beyond 40 km a ZIP has no weather rather than a
distant city's. No elevation correction, so a hill town matched to a valley station
reads a little warm; the Climate cell's hover gives the station distance. 99.9% of ZIPs
get temperatures, 98.4% snow.

## 2026-09-27 — Hazards are FEMA's expected building loss per dollar, not its ratings
**Decided.** The National Risk Index's headline ratings multiply expected loss by the
local population's social vulnerability, so the same flood rates riskier in a poorer
tract — the income correlation `crime_index` was thrown out for — and they are relative.
The board uses expected annual building loss ÷ building value per tract, apportioned to
ZIPs by land area (Census 2020 ZCTA–tract file). "Not Applicable" reads as $0 (no coast,
no coastal flooding); "Insufficient Data" is missing; a rating the code has never seen
stops the refresh. A ZIP with under half its land in scored tracts has no figure.

## 2026-09-27 — Hazard filters are in dollars, not national ranks
**Decided.** The first cut ranked every ZIP nationally ("avoid the worst 10%"). Most of
these distributions sit near zero — the median ZIP's wildfire loss is 13 cents a year per
$100k — so a rank turned $1 a year of earthquake loss into "82/100". Filters and the
Hazards column now read dollars a year per $100k of building. Thresholds, with the share
of the 25,511 scored ZIPs each removes:

| Filter | Choices ($/yr per $100k) | Removes |
|---|---|---|
| Flood | 200 / 150 / 100 | 6% / 14% / 35% |
| Wildfire | 50 / 10 / 2 | 4% / 11% / 22% |
| Hurricane & tornado | 100 / 50 / 25 | 4% / 8% / 17% |
| Earthquake | 50 / 25 / 10 | 9% / 13% / 19% |

The column sums all four and turns amber at $250 and red at $500 — roughly the national
90th and 99th percentiles of the total — naming the biggest hazard only then, because
flood is the baseline almost everywhere. The page shows each hazard's national median
from the data file, so "typical" moves when FEMA's figures do. Dollars are per $100k of
BUILDING; the hover applies them to the row's purchase price as an upper bound, since
the price includes land.

## 2026-09-27 — "All states" prices each row with its own state; hides the scenario card
**Decided.** The national board computes every row with that row's state — tax and
insurance in the PITI, durable cap rate, FBI safety, and the structural flags behind the
trajectory veto — so a ZIP's row matches its own state's board; the test samples one ZIP
per state and compares. The scenario card and the listing checker are hidden nationally:
both need one state's tax and insurance rates, and `_fha_piti("ALL")` silently falls back
to defaults. Each row's state links to that state's board with the filters kept.

## 2026-09-27 — Weather and hazards are separate JSON layers, refreshed quarterly
**Assumption.** `data/zip_climate.json` and `data/zip_hazards.json` are joined by ZIP at
request time rather than written into `zips.db`, which `build_national_zips` rebuilds
monthly from other sources. Separate files keep each source's refusal guards and
coverage history independent. Both change slowly (normals once a decade, the NRI about
yearly), so the jobs run quarterly; a filter is offered only for places its file has
figures for.

## 2026-09-27 — ZIPs join HUD counties by state + county name
**Assumption.** HUD's county FMR is keyed by FIPS code; `zips.db` stores the county's
NAME. Rather than add a crosswalk, ZIPs join HUD's own county list on state plus name —
unique within a state — after spelling-only normalisation: accents, case, periods,
apostrophes, spacing, and Saint/St. The words themselves are never touched, so
"Richmond city" and "Richmond County" stay two places. A name matching two counties, or
none, gets no FMR and is logged. The first national run matched 25,346 of 25,769 ZIPs;
the misses were spelling, which the Saint/St. and spacing rules now cover (a re-pull of
the 14 affected states gave 209 more ZIPs a rent and changed nothing elsewhere). What
remains is Connecticut (HUD now publishes planning regions) and a few Alaska boroughs
whose names differ in substance — 278 ZIPs. (Connecticut and the rest of New England
now join by town instead — 2026-09-28, below.)

## 2026-09-27 — HUD rents are labelled, not adjusted; strained ones are flagged
**Decided.** HUD's 2-bedroom Fair Market Rent now answers for 17,223 ZIPs Zillow doesn't
cover (measured-rent coverage 33% → 99.4%; 148 ZIPs have none). Checked against Zillow where both exist
(FY2027): SAFMR is 1.01× Zillow's at the median (a quarter ≤0.89×, a quarter ≥1.12×);
county FMR 0.93×. So it is not the "floor below market" the ladder's caveat claimed —
FMR is GROSS rent, and utilities offset the 40th percentile — and the caveat now says
what the data showed. The ratio climbs as local incomes fall: where HUD's rent is 40%+
of the ZIP's median household income, county FMR runs 1.11× Zillow and 3 in 10 are 20%+
high (588 HUD-rent ZIPs are in that band). Those rents are flagged (amber "HUD !") rather than scaled down, because a scaled
number has no source you can name, and a landlord renting to voucher holders can in fact
be paid HUD's figure. Every HUD rent carries a tag whose hover says what it is.

## 2026-09-27 — A HUD run is authoritative only where it asked
**Decided.** A limited run (`--states OH`), a state whose county list failed, or a county
whose request failed keeps its stored HUD rents instead of blanking them; more than 5%
of counties failing discards the pull entirely. Stored county FMR is carried back as
FMR, never promoted to SAFMR. Same principle the ladder already applied to whole
sources, now applied per state and per county.

## 2026-09-28 — Census ACS comes from the Bureau's bulk files, not the keyed API
**Decided.** The Census data API now refuses keyless requests, and a key requested for
this repo never activated ("Invalid Key" after the validation page failed). The same
5-year tables are published as one pipe-delimited file per table, for every geography,
on www2.census.gov with no key. `acs_bulk.py` reads them (ZCTA rows by default, states
by prefix), renames columns to the API's spelling so the derivations didn't change, and
picks the newest vintage on the server (2024 today; the code had been pinned to 2022).
The ZIP build, the rent ladder's ACS tier and the state job all use it; `CENSUS_API_KEY`
is gone. A bulk-file outage still carries the prior values forward, but as a GitHub
warning — the silent carry-forward is how the ACS columns stayed 0% populated for
months while the job went green.

## 2026-09-28 — The monthly ZIP rebuild carries the rent ladder across
**Decided.** `build_national_zips` deletes and recreates `zips.db`, and the columns
`refresh_rents` adds weren't in its schema — so HUD rents vanished on the 1st of every
month until the rent refresh on the 2nd, and for a month if HUD failed that day. The
rebuild now snapshots the stored per-source rents first and re-resolves every ZIP with
`refresh_rents.apply` (fresh ZORI, carried HUD/ACS). Verified on the real rebuild: of
the ZIPs present before and after, none lost a rent or changed source.

## 2026-09-28 — "Young adults" and "2–4 unit buildings" are filters, from ACS
**Assumption.** Young adults = share of residents aged 25–34 (B01001), thresholds 12 /
15 / 18% (the typical board ZIP is 12%; 18% keeps the top 8%). "Young professional" is
left as a combination the user builds — this plus degree share plus renter share —
rather than a composite with invented weights. 2–4 unit buildings = share of housing
units in 2- and 3–4-unit structures (B25024_004 + _005), thresholds 5 / 10 / 15% (the
typical ZIP is 4%). Both are offered only where the column has data, like the other
Census filters.

## 2026-09-28 — The safety gate uses every city police department's FBI reports
**Decided.** `crime.json` was 394 hand-researched cities, 128 of them flagged suspect —
about 213 usable nationally, so the safe filter emptied most boards. It is now built
from the FBI Crime Data Explorer's own backend (no key) for every city agency that
matches a ZIP city: 8,440 agencies, 2023–2025, 6,977 usable cities. Rules
(`crime_build.py`, all tested and mutation-checked):
- A year counts only if BOTH violent and property crime were reported all 12 months;
  nothing is scaled up from a partial year.
- Cities of 10,000+ use their latest complete year; smaller towns pool their complete
  years so one incident can't move a tier.
- A latest year under 40% of the agency's own prior average (on a base of 10+), or zero
  violent crime across every complete year in a town of 5,000+, is suspect — a reporting
  change, never a safe label.
- Agencies tie to ZIP cities by state + spelling-normalised name, within 40 km of the
  city's ZIPs; a city agency beats a township of the same name; ambiguity gets nothing.
- A researcher's "suspect" is lifted only by an FBI figure that passes every rule and is
  at least 100 per 100k (75 lifted, e.g. Chicago, Columbus). Below 100, under-reporting
  and genuine safety look the same, so those doubts stand (27, e.g. Savannah at 86,
  Cheshire CT at 6).
- Each yearly run starts from the last table and is stable on its own output; an FBI-only
  city not matched in a later run is dropped rather than kept on an old figure.

## 2026-09-28 — The FBI pull runs on ten runners at once
**Decided.** The CDE answers a request in ~0.3s, but one runner making every request
crawled: a 90-minute run never finished. Two causes, both measured: a fresh connection
per request met connect hangs (~1 in 25), fixed with one persistent connection per
worker; and sustained volume from one runner slowed further, fixed by splitting the
agencies ten ways (`--shard i/10`) and merging with checks for a missing or duplicated
shard. The national pull now takes about four minutes.

## 2026-09-28 — Every suite runs on every pull request, database halves included
**Decided.** `.github/workflows/test.yml` gained an `all-suites` job: install
`requirements.txt`, start a throwaway Postgres 16 service, run `tests/run_all.py`. Eight
suites have a database half that skips without `DATABASE_URL`; the runner gives each its
own fresh database (they migrate down and up, so they can't share) and fails a suite that
still reports "no DATABASE_URL" when one was provided — a skip that reads as a pass is
the failure mode being closed. The existing `household-engine` job is left as it was in
case it is a required status check; it is now redundant.

## 2026-09-28 — Connecticut's tracts join by their unchanged tract code
**Decided.** FEMA's NRI carries Connecticut's 2022 planning-region tract IDs (county part
110–190); the Census 2020 ZCTA-to-tract file the hazard join uses carries the old county
ones (001–015), so no CT ZIP had a hazard figure. The renumbering changed only the county
part, and the 6-digit tract codes are unique statewide — checked against the live data
before any code: all 879 of FEMA's CT tracts matched exactly one 2020 tract, and every
2020 CT tract was matched. `recode_tracts` renumbers by that code, in Connecticut only
(elsewhere a tract code repeats across counties), and leaves anything ambiguous unjoined
rather than guessed. Result: 256 of 256 CT ZIPs have hazard figures (was 0); no other
ZIP's figures changed.

## 2026-09-28 — New England ZIPs take HUD's rent for their town, not their county
**Decided.** In CT, MA, ME, NH, RI and VT, HUD sets Fair Market Rents by town, and one
county can hold several rent areas: Worcester County, MA spans four ($1,659–$2,499 for a
two-bedroom); in Middlesex County, Boston's is $3,008 and Lowell's $2,383. The refresh
keyed each town's figure by its county, so every ZIP in a county took whichever town HUD
listed last. Connecticut got nothing at all: HUD's town list still returns pre-2022
codes, and its data endpoint answers 404 to every one of them.

HUD's own ZIP-to-town crosswalk (`/usps?type=11`, same token, one request per state)
now places each ZIP, by its share of residential addresses, and supplies each town's
current code. A ZIP takes its main town's FMR when that town holds at least half its
homes; a ZIP split more evenly takes one only if every town it touches has the same
figure — straddling two rent areas, it gets none rather than a coin flip. Towns the
crosswalk puts no ZIP in aren't requested (34).

First run (FY2027): Connecticut went from 139 ZIPs without a rent to none — all 256 have
an FMR and 90 a SAFMR (Hartford is a small-area metro). The FMR changed for 119 MA, 47
ME, 47 NH, 13 RI and 7 VT ZIPs; Lynn, Salem and Peabody, for example, moved from $2,367
to Boston's $3,008. Four ZIPs lost their FMR (three straddle rent areas; one is Essex,
VT, which keeps its Zillow rent), and two small ones — populations 838 and 1,531 — now
have no rent. The same run filled the ACS tier for the first time since the keyless
switch (23,238 ZIPs), so ZIPs with no measured rent nationally fell from 394 to 171.

## 2026-09-28 — The compounders' "wrong prices" were share counts; restated at source
**Decided.** The P0 read "`refresh_compounders` writes a wrong price" (Booking at 0.8x
P/FCF). The prices were right — Yahoo's live quote. The share counts under them weren't,
for two reasons, both fixed in `refresh_compounders.py` and tested against the SEC's own
figures:
- **Stock splits.** Yahoo's prices are split-adjusted; SEC counts are as filed, and a
  filing never restates for a later split. Booking split 25-for-1 on 2026-04-06, after
  its 10-K reported 32.6M shares, so its $164 price read 0.6x. Each count now carries its
  filing date, the 10-year chart supplies the split history, and every split after the
  filing is applied. Domestic filers only — an ADR's splits aren't its ordinary shares'.
  A count filed before the split history begins is dropped, not used unrestated.
- **Counts filed at the wrong scale.** McDonald's tagged its FY2023–25 count as 732.3
  (millions); ConocoPhillips filed 2015–21 in thousands. A year-over-year jump within 3x
  of 1,000 or 1,000,000 is read as a change of unit, and the series is anchored on the
  latest count being a size a listed company can have (1M+ shares). Run after the split
  restatement, so a 1-for-1,000 reverse split is never mistaken for one.

First run: 300 companies restated for a split, 73 rescaled. Booking 0.6x → 14.7x,
McDonald's 0.0x → 23.6x, Chipotle's historical median 1.2x → 52.7x. Companies whose
share count appeared to shrink 20%+ a year — and so took the full buyback bonus in the
score — fell from 72 to 8; P/FCF medians under 2x from 19 to 6; current P/FCF under 2x
from 29 to 21. The `/holt` and FCF-quality guards stay: they now catch what's left
rather than the bulk of it.

## 2026-09-28 — SEC figures are read as companies file them today
**Decided.** The audit of the five screener pages found one cause under most of their
wrong numbers: a tag read the way a company filed it years ago, or not read at all. Fixed
in `refresh_compounders.py` (Compounders, FCF Quality), `lynch.py`/`lynch_screener.py`
(Lynch) and `hundred_screener.py` (100-bagger), each against the SEC's own figures:

- **Debt is read on one date, the latest balance sheet**, and assembled. Each component
  used to take its own latest value, so a retired tag supplied a 2011 figure beside this
  year's cash (Home Depot $18.1bn → $51.3bn, AT&T $70.3bn → $136.1bn, Micron $3.3bn →
  $14.6bn), and `LongTermDebt` was thrown away whenever a current portion was filed (Union
  Pacific $1.5bn → $31.8bn, AbbVie $8.6bn → $65.0bn). A combined total tag (US GAAP, or
  IFRS `Borrowings`) is the filer's own figure and stands — some filers tag one amount
  twice, and adding their parts doubles it (Lumentum, Cenovus) — unless it is under a
  quarter of the debt due this year or of the parts (ON Semiconductor's $0.9m beside
  $2.98bn). The price is a filer whose total leaves something out: Home Depot's $51.3bn
  omits its $4.5bn of commercial paper. Otherwise each route is a floor and the largest
  stands: noncurrent line + debt
  due within a year; `LongTermDebt` + short-term borrowings; the largest one-kind total
  (senior notes, secured debt, converts) + short-term borrowings. The ladder now carries
  the lines many filers use instead of the main ones — finance-lease lines, converts,
  senior notes, secured/unsecured totals, IFRS `Borrowings` and its parts (Toyota, POSCO,
  KEPCO and Anheuser-Busch had been read at a fraction of their debt; GM and BP at zero;
  Ball at $2m against $7.0bn).
- **Unknown is not zero, and zero is not unknown.** Only short-term debt on the latest
  balance sheet, or a one-kind total smaller than the debt due this year, from a filer that
  tagged long-term debt before, is unknown rather than read as the whole (Deere). A company
  whose last debt figures were all zero and that has filed none since is debt-free
  (Copart, Lululemon, Vertex, Signet), with the date it said so in `debt_zero_as_of`.
  Either zero inferred from what was NOT filed stands only while the year's interest is
  under 0.25% of revenue — Caleres, with a revolver under a tag this list doesn't read,
  pays 0.64% and stays unknown. Cash is read on the same date, plain `Cash` included
  (SLB's had come from 2014).
- **Capex and EBIT fills.** `PaymentsToAcquireOther…` fills capex years the main tags
  miss (Eli Lilly, Verizon since 2019); pre-tax income + interest fills operating income
  where that line stopped (TJX since 2019). Fill-only, same currency only.
- **Lynch.** The share count is the newest from either the balance sheet or the cover
  page (Walmart's came from 2012, before a 3-for-1 split). A twelve-month figure inside a
  10-Q is not a fiscal year (Amazon). Net income filed in millions as dollars is rescaled
  when the company's own EPS × weighted shares says so (FedEx, Medtronic; also on the
  100-bagger). The units check compares EPS with the same year's net income and passes
  if any filed count reconciles — weighted diluted, weighted basic (an Up-C's diluted
  count includes units EPS isn't divided by), or today's — at the filed scale, or at
  x1,000 / x1,000,000 within 10% (Nova's count is tagged in thousands).

Branch runs against main (compounders and FCF quality rebuilt with `--force`; Lynch and the
100-bagger rebuilt four weeks after main's snapshot, so price moves are mixed in):
- **Compounders:** rows with this year's free cash flow 1,753 → 1,787; with a net
  debt/EBIT 1,344 → 1,455. Debt read as inferred zero 239 → 62; 93 rows read as zero
  because that is what they last reported; 205 rows are unknown rather than read from a
  stale or partial tag (main read every row, many of them wrong).
- **FCF Quality:** measured 1,110 → 1,120; rows refused by the debt cross-check 48 → 4;
  large caps refused as inferred debt-free 34 → 10; 87 refused for unknown debt, 3 for
  unknown cash. The final board has 134 names (133 before), 22 of them different — mostly
  companies whose enterprise value grew once their real debt was counted (Cardinal Health,
  Sirius XM, Cable One), and GM, now priced with its $131.6bn of debt instead of refused.
- **Lynch:** rows rejected as units problems 264 → 105; unmeasured 1,109 → 943; passing
  3 → 5 (Yalla and Stride join Lululemon, Ingredion and BOS). The 21 newly failing the
  units check are real EPS/net-income mismatches — a stale net-income tag (Bloom Energy,
  Estée Lauder), Wise's EPS in pence, opposite signs (PTRN) — 19 of them confirmed in a
  probe.
- **100-bagger:** two banks priced at 75x and 67x earnings on a quarter's net income read
  as a year (First Community, Colony) now read 18x and 16x.

## 2026-09-28 — What a screen could not measure is its own answer, never a failure
**Decided.** Audit item #2: on four boards, "we could not read it" was shown as "the company
failed", "dormant" or "did not file". Each board now keeps three answers apart — passed,
failed, and could not be measured — and words the third as a limit of the reading:
- **Compounders.** Gates answer pass, fail or not measured. A company that fails nothing
  that could be checked but has a gate that could not be (ROIC on negative invested capital
  — Domino's, VeriSign; unknown debt — NVR; no readable capex — Waters, Rio Tinto) is
  UNMEASURED, not GATED: 17 rows. A measured failure outranks an unmeasured gate, and a
  missing debt ratio beside an operating loss is still a failure. Computed on page load, so
  no rebuild.
- **Lynch.** IFRS filers read "files its statements under IFRS, which this screen does not
  read yet" instead of "stockholders' equity not filed" (343 → 61 left as no_equity; 223
  IFRS, 90 funds with no company statements). The units and SEC-returned-nothing labels say
  what failed on our side. All count as could-not-measure.
- **100-bagger.** "Dormant" is kept for dollar filers whose figures stopped: 501 → 32. IFRS
  filers (369), funds with no statements (108), US-GAAP filers reporting in another
  currency (49), missing quotes and empty SEC answers move to the funnel's could-not-measure
  bucket, renamed from "too thin to judge": rejected 3,207 → 2,306. The page now says banks
  mostly land there — they file interest income, not revenue, and no gross margin or
  capital employed.
- **Every screen built on the shared universe.** One ticker per company, the first that is
  common stock: the map kept the last, so ~250 companies were screened under a preferred
  line or unit (Boeing as BA-PA, reading "no market cap"). A warrant is a `-WT`/`.WS`/`-W`
  suffix or Nasdaq's fifth-letter W; "ends in W" had dropped Lowe's, ServiceNow,
  Sherwin-Williams, Edwards, Snowflake and CDW (and T. Rowe Price, for boards without the
  sector cut) from every screen, uncounted. Lynch screened 3,321 → 3,550, the 100-bagger
  5,182 → 5,525; neither list changed in size (Lynch's five; the 100-bagger's 33, one swap).

Not done here (BACKLOG): actually reading IFRS statements and bank revenue on these boards,
partnership equity, and telling a fund's 404 from a failed request.

## 2026-09-28 — Aristocrats: dividend safety is measured from filings, and unknown is never safe
**Decided.** Audit item #3. The safety gate read a missing payout or leverage as a pass, and
both were hand-seeded for only 22 and 16 of 92 names, so four of the five BUYs had neither. The
monthly refresh (`refresh_aristocrats.py`) now computes both from SEC companyfacts:
- **Payout** = cash dividends paid over the BETTER of two three-year covers: earnings and
  free cash flow (REITs and midstream may also use earnings + D&A, an FFO proxy). One year's
  charge cannot decide it — AbbVie's GAAP payout reads 248% on acquired R&D and 58% on cash;
  a utility's negative free cash flow simply leaves the earnings cover. Dividends paid while
  every cover was negative (Albemarle) is a failure. Lines 75% warn / 90% hide: the trailing
  covers ran ~9 points above dividend.com's forward seeds, which keep their 65/80 lines where
  SEC has no figure (SJW, Canadian Utilities).
- **Leverage** = net debt on the latest annual balance sheet (the compounders build's debt
  assembly) over EBITDA, taking the higher of the latest year and the three-year median so
  one impairment year does not gate (Air Products), and adding intangible amortization where
  a filer tags it apart from depreciation (AbbVie). Lines 2.5x/3.5x; 5.5x/6.5x for utilities,
  REITs and midstream, which run regulated or contracted cash flows at those levels as a
  matter of course; not applicable to insurers (Chubb, Aflac, Cincinnati Financial). A debt
  figure that is one kind of borrowing only is a floor: it can prove a failure, never a pass.
  A balance sheet over 21 months old is not measured.
- **Status.** A cheap name that clears Chowder but whose payout or leverage could not be
  measured is UNMEASURED, never BUY.

Branch refresh: payout measured for 79 of 92 names, leverage for 73. BUY is now Lowe's (payout
36%, 3.2x — warn), California Water, Middlesex Water, ADP and NextEra (6.1x — warn); Brown &
Brown, a BUY with no figures, is gated at 3.8x after its acquisition borrowing. Becton
Dickinson (3.7x) and Stanley Black & Decker (3.9x) gate on GAAP EBITDA, which runs below the
adjusted EBITDA most published leverage figures use — the page says which basis it measured.

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

## 2026-09-28 — FCF Quality: a broken multiple is held out of the ranking, not badged
**Decided.** Audit item #4. The build ran holt.py's P/FCF fault test on the finished board and
only drew a "check price" badge, so the board's top two — Fiverr at 49.8% (3.2x against its own
38.5x median) and Shutterstock at 38.0% (1.8x, under the 2x floor) — ranked first while
flagged. The badge also misdirected: the fault is usually the share count or the cash-flow
figure, not the price. The test now runs inside `fcf_quality.measure()`: a row that fails it is
seen, counted as not rankable, kept out of the growth-rank cohort, and listed under the table
with the reason. Only the flattering side is refused — holt's 2,000x ceiling is a yield under
0.05% here, sorts last on its own, and is more often a real company with little free cash flow
that year (Teleflex, Fabrinet) than a bad input. `holt.py` joins the build's logic hash, since
it can now change the board.

Branch rebuild: 17 held out (10 had passed both stages), 132 pass (was 134). The top of the
board is now G-III 34.0%, Weibo 31.8%, Collegium 29.6%.

## 2026-09-29 — Owner design calls (#5): Lynch on PEG, 100-bagger requires growth, no re-rating credit
**Decided by the owner**, from options measured on the committed data (audit item #5):
- **Lynch: PEG ≤ 1, with P/E ≤ 20 as a backstop** (was P/E ≤ 10). PEG ≤ 1 is Lynch's own test; the
  single-digit P/E was a stricter value tilt that stopped 91% of the companies reaching it. The 20x
  ceiling stays because the trailing growth rate is uncapped. `PE_SANE`'s unused upper value is now
  `PE_MAX`. Rejected rows keep P/E, growth and PEG so the next rule change can be measured offline.
  Rebuild: 5 → 28 passing; all five previous names still pass. `pe_high` 1,119 → 759 rejections.
- **100-bagger: revenue growth must score Good or better** (15%/yr, the checklist's own line). The owner
  chose revenue rather than "revenue or EPS", accepting that steady earnings compounders with slower
  sales growth (Pool, Genpact, SAIC) leave. Growth this screen cannot read goes to could-not-measure.
- **100-bagger: ROE at the 100% bound and P/E under 5x are unmeasured, not Great.** Neither casts a vote;
  the low P/E still counts in its peer median. Rebuild: 33 → 15 listed, exactly the offline estimate
  (Embecta, MIND CTI, Brinker, Novavax, SANUWAVE, Exzeo and 12 others out; none in).
- **Compounders: no credit for re-rating up** toward the 7-year median P/FCF, which includes 2020-21; the
  −3%/yr penalty for paying above it stays. COMPOUNDER 8 → 2 (LKQ, Lennar); Adobe, Sirius XM, Gartner,
  Pool, Maximus and National Beverage stay on the page as QUALITY. The page said "10-yr median"; it is 7.

- **Lynch growth guards tightened** (owner, after the first rebuild showed 7 of 28 passes at 50%+/yr, most
  of them recoveries). Lynch board only; the 100-bagger keeps the defaults. When the base year sits below
  the highest of the three years before it, the rate is measured from that peak; a latest year more than
  100% above the one before (was 200%) is re-measured without it. Both only lower a rate. Rebuild: 28 → 14.
  Out on an honest rate under 10%/yr: A.O. Smith 6.3%, Covista, Cirrus, Yum China, Spectrum, YETI, Dorman,
  El Pollo Loco, Graham Holdings, Disney (−17% from its 2018 peak), Interface 6.5%, Optex, Mueller Water,
  Marzetti. Lululemon, Limbach, Yalla and Genpact stay at their lower from-peak rates, badged.
- **100-bagger verdict order:** a measured revenue-growth failure is now a reject even when too few
  criteria were read to count; 377 rows move from could-not-measure to rejected. The list is unchanged.

Not done (BACKLOG): measuring the drift against a median without 2020-21 (needs per-year P/FCF stored).

## 2026-10-01 — Map rebuild, phase 1: a ZIP dataset of measurements, and the underwriting arithmetic
**Decided by the owner:** rebuild the map and ZIP page from scratch for BOTH rental investors (single-family
and 2-4 units) and owner-occupants, covering all US ZIPs, with no composite scores or personas. The audit
found the old ones ranked ZIPs by density/income/education proxies labelled "walk", "restaurants" and
"crime", put a flat-40% "cap rate" on a voucher rent, and charted a forecast that does not beat "last
year repeats".
- **A new file, not a migration.** `data/zip_profile.db` is built beside `zips.db`, which eleven other
  pages read; the old map keeps working until the new one replaces it (later phases).
- **Every Census ZCTA in the 50 states + DC (32,793)**, with state and county from the Census
  ZCTA-to-county file (largest land share), not only Zillow's 26,210.
- **Census placeholders are not numbers.** A median whose margin of error carries the open-interval
  annotation (-333333333) is stored NULL with its bound in `acs_flags`. Confirmed on the 2024 vintage:
  incomes $250,001, taxes $10,001, rents $3,501, owner costs $4,001; 5,007 ZIPs carry at least one.
- **Property tax per ZIP** = Census median taxes paid ÷ median owner value (an existing-owner rate).
  Investor default: that rate scaled to the state's investor purchase rate (proptax.json); states where
  the assessment resets at sale (CA, FL, OK, AR, NM, SC, MI) use the state purchase rate flat. Owner
  default: the ZIP rate, floored at 1.10% in California. Where the Census top-coded taxes at "$10,000+"
  (925 ZIPs, 4.9% of residents, mostly NY/NJ/CA; 829 have a median value to work from) the ZIP rate is
  the greater of the floor that implies
  and the county's measured median — Manhattan 10009 1.40%, not New York's upstate-driven 1.95%.
- **Insurance**: state DP-3 landlord and HO-3 owner premiums at a $300k dwelling, scaled by price within
  0.6-2.0x (land is not insured). `re_assumptions.py` is now the one place these defaults come from.
- **Rent**: Zillow asking rent (8,434 ZIPs) is the only rent price-to-rent is computed on; HUD and Census
  rents by bedroom are carried for the underwriting card, labelled by source.
- **Underwriting** (`underwrite.py`): investor operating statement line by line, NOI, cap rate, GRM,
  1% rule, debt service at the owner rate + 0.75, cash flow, cash-on-cash, DSCR, break-even rent and
  occupancy, leverage spread; owner PITI with PMI over 80% LTV, income needed at 28/36, cost of owning
  (interest, not principal) against rent. Defaults carry their convention in `PROVENANCE`; a missing
  input leaves its figures empty and is named.
- **Redfin ZIP market activity is feasible but stale**: the tracker is 1.55 GB, 9.7M rows, streams in
  82 seconds on a runner, but its latest period ends 2026-05-31 (file last modified June 2). Phase 2
  must date every figure.

## 2026-10-01 — Map rebuild, phase 2: market activity, every figure dated
**Probe (2026-10-01):** every Redfin market tracker is frozen — the monthly ZIP, county, metro, state and
national files end 2026-05-31 and were last written 2026-06-02; the weekly file ends 2026-04-26. The
existing state "days on market / sale-to-list" layers on the old map read Redfin's state file, so they
are four months old (BACKLOG). Realtor.com's Inventory Core Metrics are current (September 2026, written
2026-09-30) by ZIP, county and metro; Zillow's metro market files run to August.
**Decided:**
- **Listings from Realtor.com, by ZIP (28,558) and county (32,464 ZIPs' counties):** active, new and
  pending listings, pending ratio, median days on market, share with a price cut, list price and list
  $/sqft, with year-over-year change. '_yy' on a count or price is a fractional change (stored as %), on
  a share it is a change in points (stored as points). Attribution: Realtor.com Economic Research.
- **Sales from Redfin, frozen and dated:** median sale price, homes sold, $/sqft, sale-to-list, sold above
  list, months of supply, days on market (90-day windows ending 2026-05-31; all residential, plus
  single-family and 2-4 unit sale price and count). Only windows ending on the file's final period count —
  3,302 ZIPs Redfin stopped reporting (some in 2012) carry no "latest" sales.
- **Thin:** under 10 listings or 10 sales in the window is flagged thin, not hidden (12,310 ZIPs' listing
  figures). **Realtor.com's own quality_flag** is kept: 14,700 ZIPs carry it, 8,798 of them with under 10
  listings; of 979 ZIPs with 200+ listings only 116 are flagged, typically with double-digit year-over-year
  list-price swings. The page will badge flagged figures as volatile rather than drop them.
- **A failed source never blanks the board:** its columns carry forward from the previous build with their
  own months, and meta records which source was carried.

## 2026-10-01 — Map rebuild, phase 3: the ZIP page reads zip_profile.db, and every figure says what it is
**Decided:**
- **`/zip/{zip}` is rebuilt on zip_profile.db** (`zip_page.py` + `templates/zip_profile.html`); the old
  page (`zip_detail.html`, its composite scores and forecasts) is gone. Any of the 32,793 ZCTAs gets a
  page; one the Census does not map gets a plain not-found page.
- **Price and rent must describe the same home.** The card offers only pairings with both halves:
  Zillow typical home + Zillow typical rent (default where both exist); HUD n-bed rent with Zillow's n-bed
  value; Census n-bed rent (labelled existing tenants' rent, which lags the market) with the same value.
  With no Zillow value the price is the Census median owner value, and the note says so. Default order:
  Zillow, HUD 3-bed, Census 3-bed, HUD 2-bed, Census 2-bed. HUD rent is named as county FMR or ZIP-level
  Small Area FMR (40th percentile, utilities included).
- **Every input is editable and recomputed server-side** (`/api/zip/{zip}/underwrite`, the same
  `underwrite.py` the page renders with — no second copy of the arithmetic in JavaScript). Typed price or
  rent beats the pairing; a junk value falls back to the default; units stop at 4 (above that is
  commercial lending); no mortgage rate means the card says so rather than assuming one.
- **Break-even occupancy over 100% reads "rent can't cover costs"**, not 427%.
- **Labels:** Redfin's window is badged stale past 3 months (it is May 2026 for everyone until Redfin
  resumes); a listing figure under 10 listings is "thin"; Realtor.com's quality flag is "volatile";
  a Census top- or bottom-code reads "$10,000+" / "1939 or earlier", never as a measured figure.
- **Comparisons are medians across ZIPs** (county, state, US) with how many ZIPs; thin listing figures stay
  out of the medians; "rank in state" is the share of the state's ZIPs at or below this one.
- **Series storage moved to `data/zip_series.db`** (one zlib-JSON blob per ZIP and series) so a page
  reads one ZIP's history without loading the national file; replaces `zip_series.json.gz`.

## 2026-10-01 — Map rebuild, phase 4: one measure at a time, on one basis, every ZIP
**Decided:**
- **`/map` is a ZIP map of all 32,793 ZCTAs** (`map_data.py` + `templates/map.html`), coloured by one of
  55 measures in eight groups: investor and owner figures underwritten at stated defaults, prices,
  rents, Realtor.com listings, Redfin sales (badged stale), Census people and housing, FEMA/NOAA risk
  and climate. No score, no persona, no forecast. The previous map (metro pins, composite scores) moves
  to an unlinked `/map/classic` until phase 5 retires it with the other composite readers.
- **One price/rent basis per map.** Investor figures are computed on Zillow's typical home and rent
  (8,404 ZIPs) or on Zillow's 3-bed value with HUD's 3-bed rent (20,159), chosen by the reader — never
  Zillow rent in one ZIP and HUD rent in the next, which would colour the map by which source exists.
  Owner figures use Zillow's typical home. The arithmetic is the ZIP page's (`underwrite.py`, same
  defaults, same tax and insurance), and a test holds the map and the ZIP page to the same cap rate.
- **A year's rent over 20% of the price is flagged, not dropped** (`underwrite.IMPLAUSIBLE_GROSS_YIELD_PCT`):
  47 Zillow-basis ZIPs, 136 on the 3-bed basis — Sag Harbor's $57,000/month seasonal rent, Flint's $29k
  distressed value against a rentable home's rent. Flagged and thin figures are drawn hollow and left out
  of the colours, medians and table unless the reader includes them; the ZIP page shows the same warning.
- **Colours are quantiles of what is shown** (7 groups), recomputed for every state and filter, so the
  legend always splits the visible ZIPs evenly and prints its own ranges and counts.
- **Filters are measure ranges** (any measure, min/max); a ZIP without a figure for a filter is left out,
  and the page says so. The view lives in the URL, so it can be shared; the table exports to CSV with
  each figure's flags.
- **Census top-codes are sent as their bound and marked**, so a ZIP at "$250,000+" sorts at the top and
  reads "$250,000+", rather than vanishing as missing.
- **Realtor.com's quality flag** marks about half of ZIPs some months: a dagger and footnote in the table,
  the full "volatile" badge in the popup.
- **Points, not polygons**: ZIP centroids on a canvas layer. National ZCTA boundaries are ~60 MB; see
  BACKLOG.

## 2026-10-01 — Map rebuild, phase 5: the composites and forecasts are gone
**Decided by the owner:** retire the old map, its metro maps and the composite/forecast columns; leave the
other boards' inputs (the walk/restaurant/crime proxies and the flat-40% cap rate on /norcal,
/multifamily and /value-add) as they are, logged in BACKLOG [BOARD-PROXIES].
- **Deleted:** the old national map (`national_map.html`, served at /map/classic since phase 4), the 112
  hand-curated metro maps (`state_map.html`, `state_neighborhoods.py`, `dallas_neighborhoods.py` — the
  persona composite scorer), and `/api/zips`, `/api/zips/stats`, `/api/search`, which only those pages
  called. `structural.state_trajectories` (read only by the old map) is gone.
- **Redirected, not broken:** /map/classic → /map; /real-estate/{slug}/map → /map?st={state} (301).
- **zips.db:** dropped `composite_{balanced,investor,lifestyle,score}`, the nine `forecast_*` columns and
  the composite index (55 → 42 columns; every kept value byte-identical). `build_national_zips.py` no
  longer computes them (the damped-Holt forecast is deleted) and writes `cap_rate_pct` with
  rent_ladder's arithmetic, which the rebuild already re-ran on every ZIP; `refresh_rents.py` no longer
  rescores. `history_zhvi` stays: /multifamily, /headroom and /fair-value read it.
- **refresh_zillow.py keeps only the state section** data_providers reads; its per-ZIP section existed for
  the metro maps' ~480 ZIPs and would have failed with them gone ("no target ZIPs").
- **/norcal's deal check judges safety by FBI figures**, as its screen already did. A listing outside the
  screen's top tiers was being gated on `crime_index`, the proxy the screen had dropped.
- **Out of scope:** /global-values' country composite (countries, not ZIPs) and its market-cycle refresh.

## 2026-10-01 — Affordability, phase A: a fixed 2019 baseline in the ZIP profile
**Why:** the /fair-value audit found its verdict was set by the calendar (a baseline that rolls forward
five years, so 3% money turns into 6% money within a year), compared a statewide value against a
median-ZIP baseline, and gave 789 ZIPs a "2021" baseline from 2022-24. The owner chose to rebuild it as
a payment-to-income page on the ZIP data (phase B); this phase lays down the baseline.
**Decided:**
- **The baseline is 2019, fixed** — the last full year before the pandemic and the 2020-21 rate collapse.
  It never rolls. Each input is that year's own figure: the ZIP's 2019 average Zillow ZHVI (all twelve
  months or none; 23,919 ZIPs), Census ACS 2015-2019 median household income in 2019 dollars (30,651
  ZCTAs; 15 top-coded, 9 bottom-coded, marked), and the 2019 average of Freddie Mac's weekly rate
  (3.936%). 23,353 ZIPs carry price and income at both ends.
- **Non-overlapping Census periods:** 2015-2019 against 2020-2024, the Bureau's rule for comparing two
  5-year estimates.
- **Today's income in today's dollars:** CPI averages for 2019 and 2024 and the latest month (CPI-U
  255.653, 313.698, 334.131 for 2026-08) are stored so the page can bring 2024-dollar income forward,
  assuming no real growth since — stated, not hidden.
- **Keyless sources:** 2015-2019 predates the table-based bulk files (they begin with 2017-2021) and the
  Census API refuses keyless requests, so the build reads the sequence-based summary file (B19013 is
  sequence 0058, column 177; ZCTAs are summary level 860). FRED is read with the existing secret, or its
  keyless CSV.
- **Failure carries forward:** a failed Census pull carries the previous build's 2019 incomes; failed FRED
  constants carry the previous meta. Neither blocks the profile.

## 2026-10-01 — Affordability, phase B: /fair-value becomes housing affordability
**Decided:**
- **The page answers one question:** how much of the median household's income the payment on the typical home
  takes, today and in 2019 (`/housing-affordability`, `affordability.py`). The payment is the ZIP page's owner
  card at its defaults — 20% down (no PMI), 30-year fixed, the ZIP's owner tax rate, the state HO-3 premium
  scaled to price — so a ZIP reads the same on both pages. 30% is the line (HUD's cost burden; the Atlanta
  Fed's affordability benchmark). "Affordable price" is the price whose payment takes exactly 30%, solved in
  closed form within each insurance-scaling band.
- **Today's Census income is brought to today's dollars by CPI** (2020-24 median in 2024 dollars × CPI
  latest/2024), assuming no real growth since — stated on the page. The ZIP page's payment-to-income and the
  map's measure use the same income, so all three agree (Lakewood 44107: 35.9% everywhere).
- **States and the nation are a typical household, not an average of ZIPs:** household-weighted medians of
  price, income and tax rate over the ZIPs with every input in both years, run through the same arithmetic —
  the same ZIPs, weighted the same, at both ends. US today 34.1% against 23.1% in 2019 (98.1% of households
  covered); the ZIP map's median counts each ZIP once and reads lower (29.6%), which the page explains.
- **Why it changed is a Shapley decomposition** over prices, rate, incomes and insurance — each factor's
  average effect over every order of change — so the parts sum to the change exactly (US: +11.8 prices,
  +7.7 rate, −9.0 incomes, +0.5 insurance = +11.1 pts). 2019 insurance is today's premium deflated by CPI
  (premiums outran CPI, so 2019's payment is if anything overstated).
- **/fair-value 301s to /housing-affordability** (keeping ?state=); `fair_value.py` and its template are deleted;
  the nav reads "Affordability". The ZIP map gains the 2019 share, the change and the affordable-price gap;
  a "$250,000+" Census income marks a ZIP's share as "at most".
- **No warm-up thread:** computing 32k ZIPs in a background thread at startup starved the first request of the
  GIL (28 s to first byte, which tripped the ops fail-closed suite's start-up deadline). The arithmetic was made
  cheap instead (1.4 s, cached per build and rate) and runs on first use.

## 2026-10-01 — /conditions: current Realtor.com listings by state, each against a year earlier, no composite
**Why:** the audit found the page ranked states by a 0-100 "market climate" average of four Redfin state figures
whose tracker stopped in May 2026 — the spring peak, read in October as "latest" — with tiers that never fired at
the extremes (every state between 27 and 74), two inputs that move together (days on market and months of supply,
r = 0.80) and Montana silently dropped. The owner chose to rebuild it on current data.
**Decided:**
- **Source:** Realtor.com's monthly state and national core metrics (`RDC_Inventory_Core_Metrics_State_History` /
  `_Country_History`), fetched with the ZIP profile build and stored as `state_market` in `zip_profile.db` —
  25 months per geo (all 50 states, DC and the US). A failed or short fetch carries the previous table with its
  own months, and the page says so. So does a download that fails its checks: fewer than 51 states, the state and
  national files on different months, a month older than the table's, most states missing the latest month, or a
  shown measure blank for most states or the nation (a renamed column) — none of these may overwrite a good table.
  A `US` row in the state file is skipped rather than colliding with the national series.
- **One measure at a time, no blend:** median days on market, share of listings with a price cut, active
  listings, new listings, pending ÷ active, median list price. The map shows one; the table shows all.
- **Against the same month a year earlier**, computed here from the two months themselves (the exact month twelve
  earlier, or none): counts and prices in percent, shares in points, days on market in days, the pending ratio
  in its own unit. This removes the seasonal swing that made a May snapshot look like a hot market in October.
  Checked against Realtor.com's own published year-over-year figures (US active +5.41%, price cuts +0.87 pts,
  pending ratio −0.036, list price −1.35%) — they agree to the decimals shown. The pending ratio is stored to four
  decimals, not three: it moves in the third, and rounding each month to three then the change for display turned
  Arizona's −0.0346 into −0.04.
- **Redfin stays only for what Realtor.com does not publish** — sale price ÷ list price and months of supply —
  in a column group headed with its May 2026 window and marked frozen, read from redfin_overrides.json directly
  (`conditions.load_redfin`) so no hand-seeded fallback can pass for May data.
- **A state whose latest month lags the others is named on the page** with its own month — in the warning, on its
  table row, and in the map tooltip and detail card, which use the state's months rather than the page's.
  Realtor.com's quality flag is marked per state.
- The `market_climate_pct` composite is removed from data_providers.

## 2026-10-01 — /headroom phase 1: a house-hack offer an appraisal could support; one rent basis per board
**Why:** the audit found the house-hack board offering up to 4.8x a ZIP's median home value (29 of the top 40
rows above 2x) — a cash-flow ceiling with no link to what the building is worth — calling rows "live-free" with
no vacancy or repairs (26 of 40 went negative with the page's own 8% and 1.5%), and showing county HUD voucher
rents as "ZIP median rent". The market board said "Rents: ZORI-observed ZIPs only" while blending 7,987 Zillow
ZIPs with 10,777 HUD and Census ones, and a leftover test for the retired value÷204 imputation dropped 960 real
rents. The owner chose option B (fix the house-hack and the honesty now; rebuild the board in phase 2).
**Decided:**
- **The house-hack offer is the lowest of four limits, and the row names which:** the building's estimated value
  (the ZIP's single-family median × 1.25 / 1.55 / 1.85 for 2 / 3 / 4 units — `househack.est_building_price`, the
  estimate /multifamily already prices buildings at, labelled as an estimate); the price where the other units'
  rent, less 8% vacancy and repairs at 1.5% a year of price + remodel, covers the whole PITI; FHA's 75%
  self-sufficiency test on 3–4 units; the user's budget. Surplus is after vacancy, repairs and PITI.
- **PITI** adds FHA's 1.75% upfront MIP (financed) to the 0.55% annual MIP, and insures at /multifamily's unit
  factors (`househack.UNIT_INSURANCE_FACTOR`) instead of a separate +25% per unit.
- **House-hack rents are the rent ladder's measured answer, labelled** (Zillow ZORI / HUD SAFMR / HUD FMR county /
  Census), with a HUD rent that takes a large share of local income flagged — /multifamily's rule ("labelled, not
  adjusted"). A ZIP is in when its rent carries a ladder tier; the value÷204 test is gone.
- **The market board and the ZIP drill-down use Zillow rents only**, as the page already said: a market median
  needs one basis, and a county-wide HUD figure divided by each ZIP's value ranks the county's cheapest ZIPs
  first. A market with fewer than 10 Zillow ZIPs has no rent and leaves the board (Alaska, Delaware: 105 markets).
- **Wording:** the "fixer ask" is called what it is — a modelled price, the median × one national discount, not
  sales observed in the market; the claim that rent-to-price drives the all-red board is removed (the audit
  found headroom falls as rent yield rises). The page lists its research tables' dates and flags stale ones,
  which the freshness workflow's issue text already promised.
- **Caches:** `market_aggregates.json` is keyed on zips.db's content, not its file time (every deploy and checkout
  reset the time, so the committed copy was never reused), and written atomically. Solved boards are held in a
  64-entry LRU, one national solve at a time; ZIP-to-market assignment is computed once per database (a national
  house-hack request fell from 3.1s to 0.2s warm).
- Phase 2 (BACKLOG HEADROOM-BOARD) rebuilds the market board itself.

## 2026-10-02 — /headroom phase 2: the most you can pay and why, not a verdict against a guessed fixer price
**Why:** after phase 1 every one of 105 markets still read PRICED OUT in every BRRRR setting and PRIMED appeared in
none of 24 settings: the "fixer ask" was the median × one national discount while the remodel is priced per square
foot, so in Cleveland the modelled fixer plus a moderate remodel was 105% of ARV before financing. Exit
appreciation was the trailing 3-year price change, the "competition" column was a guess from the price trend, and
in 106 of 107 flip markets the $25k floor, not the return target, set the price without the page saying so.
**Decided:**
- **The board shows the most you can pay** for a fixer in each market and still clear the after-tax target and
  the $25k floor — in dollars, per sqft and as a % of the market's median home — and **names the limit that sets
  it** (the return target, the floor, or rent too thin to carry a refi: the one $500 more breaks). It ranks on the
  % of median, highest first; every column sorts. Where no price works, it says which limit fails.
- **No fixer price, no verdict.** The modelled fixer price, PRIMED / DEAL-DEPENDENT / PRICED OUT, the
  competition ±x knob and the declining-trend veto are gone. Fixer sales aren't in the data; the page says to set
  the number against what fixers there actually list for. It also says why cheap markets need deeper discounts:
  a remodel costs about the same in dollars everywhere (BACKLOG HEADROOM-ARV).
- **BRRRR shows what sizes the refi:** the loan the rent carries at the DSCR floor against 75% of ARV. At 8.28% the
  rent sizes it in 102 of 105 markets — the real reason cash stays in the deal.
- **Exit appreciation is the user's, 0%/yr by default** (−5% to +5%). The trailing 3-year price change stays as a
  history column and feeds nothing. With 0% the BRRRR max prices fall by a median 11% from phase 1.
- **Listings from Realtor.com replace the trend guess:** median days on market and the share with a price cut,
  each with its change from a year earlier — per market weighted by listings over ZIPs with at least 10, per ZIP
  on the drill-down and the house-hack board. The aggregates cache now keys on zip_profile.db's build too.
- **BRRRR timing and tax:** rented from the month after the remodel (rent less 8% vacancy, landlord insurance,
  maintenance, hard-money interest still running) instead of vacant until the refi; depreciation from that month;
  rental losses carried forward against later rental income (federal everywhere, state except PA and NJ) before the
  rest is released at the sale; refi points amortized by the month and the unamortized balance deducted at payoff.
  At equal appreciation these raise max prices by about 3%.

## 2026-10-02 — HOLT: one status per row, commodity producers by industry, the shareholders' own cash flow
**Why:** the /holt audit found the "refused — unusable multiple" tile counting 649 names of which 45 were data
faults (the rest had negative or no free cash flow, no price, or filed in another currency) and the list showing
1 of the 45; Northern Oil & Gas at #1 on a 31.8% sales CAGR that was the oil rebound, because 41 of 85 oil and gas
producers never tripped the margin-based cyclical flag; Range Resources "clean" with no ROIC, no conversion and no
cash-flow history; and Hess Midstream at a 2.3x historical multiple. A probe of SEC filings confirmed the last:
its per-share cash flow divided the whole partnership's cash flow by the public Class A shares, which owned 5% of
the profit in 2020 and 52% in 2025. Formula Systems, which owns about 40% of the companies it consolidates, read
5.7x the same way. Across the universe, 125 companies give more than 10% of their profit to minority holders.
**Decided:**
- **Every row has exactly one status**, all counted: clean, flagged (a gate measured and failed), not measured (a
  gate the data can't answer — never a pass, as on Compounders), refused (a multiple that can't be true: the 45
  faults, all listed), no usable multiple (counted by reason: negative free cash flow, another currency, no
  free-cash-flow figure, no per-share figure, no price, an unattributable minority share) and no growth figure.
- **Commodity producers are cyclical by industry** (owner's call: all extraction): SIC 1000-1499 — mining and oil
  & gas — and 2911 refining are flagged whatever their margins did; their five-year sales growth is a price. This
  takes 8 off today's clean board, among them Northern Oil, Hess Midstream and five gold and silver miners.
- **Not measured:** a missing ROIC, cash conversion, profit history (under 5 years), or — for a company with no
  margin figures and no industry flag — cyclicality. A measured failure still decides first (Range Resources is
  flagged as a producer, and its missing figures are listed). 10 rows move from clean to not measured.
- **The multiple uses the shareholders' own cash flow** (owner's call: probe, then fix — at the source):
  refresh_compounders reads each year's parent and total profit (us-gaap NetIncomeLoss / ProfitLoss / the minority
  line; ifrs-full ProfitLossAttributableToOwnersOfParent / ProfitLoss) and cuts that year's free cash flow to the
  parent's share before dividing by the parent's shares. A minority line within 2% of the profit (or loss) is
  ignored. A loss year has no profit split, but ownership doesn't move with it: it takes the share of the nearest
  year within three that has one (earlier first; "whole" if the minority was immaterial then) — Omnicom's
  merger-charge year, Hyatt's and General Mills' small minority lines. Only with no such year does it drop out;
  if that is the latest year, no multiple is built rather than one from a stale year. Cash conversion is the parent's cash over
  the parent's profit (Hess Midstream: 104%, was 410%). This corrects P/FCF on Compounders and FCF Quality too;
  Compounders marks such a multiple with * and says why on hover.

## 2026-10-02 — Quiet Value: the feed was blocked by our user agent, not by GitHub
**Why:** /quiet-value had been empty since Aug 29: every weekly run probed Stooq (now behind a JavaScript browser
check) and Yahoo (HTTP 429) and refused to write. The build assumed Yahoo blocks GitHub's runners — but the
Compounders build reads the same Yahoo chart endpoint from the same runners every month. A probe from GitHub
Actions on 2026-10-02 settled it: Yahoo answered 429 to Quiet Value's Chrome browser string and to Python's default
agent, and 200 to Compounders' plain named agent ("Mozilla/5.0 (market-pulse-refresh/1.0)") — all 400 small filers,
395 with a full year of daily volume, in 37 seconds on three workers. Nasdaq's historical API also answered (two
thirds of small names; no OTC). The owner chose to find a source before hiding the page; none needs a key.
**Decided:**
- Each price source carries its own request headers (pricefeed.SOURCES). Yahoo gets the plain named agent; Nasdaq
  and Stooq keep a browser string. Source order is now Yahoo, Nasdaq (new, keyless fallback, parsed from its
  newest-first string figures), Stooq. The run still probes before spending anything and records which answered.
- **The first real run showed the inputs were wrong too** — invisible while the feed was dead, and the page had never
  had data in production (quiet_value.json was never committed). Flows came from the net-net screener's single
  quarter: a P/E on one quarter's earnings (about 4x too high), a dividend yield on one quarter's dividend (about 4x
  too low), and capex/operating cash flow from discrete-quarter frames that companies don't file (198 filers had
  capex, so the capex test was unmeasured for everyone). Flows now come from the last complete calendar year's frames
  (CY2025 in October 2026), revenue merged across Revenues and the ASC 606 tags; the balance sheet stays the latest
  quarter-end.
- **Every candidate is priced** (default 4,000; about 2,750 today). The 400-name cap was sized for a feed that was
  refusing us, and the 400 smallest by assets were mostly pre-revenue shells — none passed more than 3 of 7 tests.

## 2026-10-02 — Quiet Value: tests that don't apply are withheld by SEC industry code, not passed
**Why:** seven of the 13 names on the first default view were small banks (ENB Financial, Muncy Columbia, Provident
Financial, LCNB, BV Financial, BayCom, C&F Financial). A bank's deposits are not filed as debt, so it passed "low
debt" and "net cash" on arithmetic; capex/OCF and operating margin describe a business a bank is not. The build
already meant to leave financials out — its candidate filter drops names containing "Bancorp", "Insurance", "REIT" —
but a name is all it checked, and these names don't say "bank". The owner agreed to withhold the tests rather than
drop the companies.
**Decided:**
- After the size cut, each name's SIC code comes from SEC's submissions record (about 1,400 requests, paced at 8 a
  second, about 3 minutes; SEC answered for all 1,418). A run that gets codes for under 90% of the board refuses to
  write, because without them a bank is scored as an operating company again; if none of the first 50 requests
  answer, it stops asking.
- **Not applicable is its own state, distinct from "not reported"**: no verdict and no metric, never a pass, out of
  the denominator, drawn as a "–" box whose tooltip says why. Banks and thrifts (SIC 6021–6036) and insurance carriers
  (6311–6399, which includes health plans; agents and brokers, 6411, are ordinary businesses) lose net cash, debt,
  capex and margin — their liabilities are customers' and policyholders' money. A REIT (6798) loses capex only:
  property purchases are not filed as capex, while its debt is real debt and is still tested.
- **A company SEC answers for with no code is a fund.** All 37 such names on 2026-10-02 were business development
  companies or closed-end funds (PennantPark, Gladstone, Barings BDC…). They lose net cash, capex and margin; their
  borrowing is real, so the debt test stays. One SEC did not answer for is unclassified, bounded by the 90% floor.
- Banks, insurers and funds can therefore measure at most three or four tests and the screen needs five, so they
  never clear it. The funnel says how many reached that step and why (29 of 234 on the default view).
- **An operating margin above 100% is unknown**, not high: operating income larger than revenue means the two figures
  disagree (a revenue tag that caught one line of the business, or a gain booked in operating income). Alliance
  Entertainment read 1,116% and was on the default view; four names were above 100%.
- **Dividends paid count where no per-share figure was filed**: the year's PaymentsOfDividendsCommonStock (else
  PaymentsOfDividends, which can include preferred) over today's share count. That measured the dividend for 113 more
  names (332 in all, from 219). A filed zero is a measured zero; no filing at all is still unknown.
- Result: the default view went from 13 names (7 of them banks) to 8, none of them financials.

## 2026-10-03 — Schloss: one date per balance sheet, payers by dollars paid, the non-debt bound wired in, funds out
**Why:** the /schloss audit found the impossible-book guard withheld a book value but left everything computed from
it — Elme Communities cleared all four gates at 0.165x a tangible book the row called impossible. The cause was a
balance sheet assembled from different dates: each line took its own newest quarter, so Elme's assets came from after
its property sale and its equity from before. Payers that file only dollars paid were scored as definite non-payers
(Utah Medical, Westwood, Escalade). The lease-and-payables subtraction written for lululemon was never passed in, so
428 companies passing every other gate sat in "could not read" on debt. Splits printed as dilution (Amazon +84% a
year). Exchange-traded funds, commodity pools, crypto trusts, BDCs and blank-cheque companies were scored as
companies. The owner chose this as phase 1; the page itself (lists that use the price) is phase 2.
**Decided:**
- **Every balance-sheet line comes from one quarter**: the newest in which the company filed both equity and total
  assets (else the newest with either). A line not filed that quarter is absent, not borrowed from another date —
  except goodwill, intangibles and preferred stock, which are subtracted from book and carry from an older quarter
  rather than read as zero. 1,403 companies read differently on 2026-10-03. Impossible balance sheets fell from 9 to 2.
- **An impossible balance sheet withholds everything built on it**: the asset and debt gates go unknown, and P/TB,
  P/NCAV and Riklis are not computed. The check now runs before anything reads the book value.
- **Dividends paid settle "pays"** where no per-share figure is filed (or the per-share record stopped): dollars paid
  this year or within DIVIDEND_STALE_YEARS. PaymentsOfDividendsCommonStock, else PaymentsOfDividends only where no
  preferred stock is outstanding. The cut signal stays on per-share figures. 363 companies pay on this basis.
- **The non-debt bound is fetched and passed in**: operating leases, payables (with accrued liabilities where filed
  together) and deferred revenue, from the same quarter. 290 companies' debt gates are now proved rather than unknown.
  The other direction: 26 September qualifiers (Best Buy, Kohl's, Ralph Lauren, Levi's, Newmont, Unum…) passed debt
  on short- and long-term figures from different quarters; read on one date, one part is not filed, so their debt is
  unknown and they move to "could not read". That is the rule the module already states — a known number added to
  an unknown one is not a known total — applied to dates.
- **A split between the two compared share counts withholds the dilution figure** (shown as "split"): a year-on-year
  jump within 8% of a split factor, and the two ends at least 1.8x apart. A jump the filings already restated (both
  ends on the same basis) does not. 718 figures withheld; 59 rows remain beyond ±60% a year, real heavy issuers.
- **Not a company, not scored**: names that say ETF or Acquisition Corp (the shared keyword filter, now whole-word —
  see below), and, among the 1,081 companies filing no revenue, SEC industry code 6221 (commodity pools, crypto and
  metal trusts), 6770 (blank cheques) or no code at all (all 49 were BDCs or closed-end funds). 470 dropped, each
  listed with its reason in params.build. Edge: Uranium Royalty is coded 6221 by SEC and goes with them.
- **The shared keyword filter matches words**: "etf" had removed Netflix and "spac" Park Aerospace, Howmet Aerospace,
  Extra Space Storage, Centerspace and Geospace from the Lynch, Quiet Value and net-net universes too.
- Result on 2026-10-03: 5,185 screened (5,725 in September, 470 of the difference funds and blank cheques); 244 clear
  every gate (192); 510 could not read (433).

## 2026-10-03 — /capital: the highest and best use of the next dollar
**Why:** the owner asked for one dashboard that finds the best place for their capital across the stock and real-estate
pages, weighs how easy each asset is to obtain, and says what to do with each month's pay for the best after-tax
result. The owner chose: all four real-estate paths compete (house hack, BRRRR/rentals, flips, a NorCal home); stocks
are picks only, no index core; the owner's time is charged at their hourly rate; phase 1 is the monthly waterfall
plus the ranked board.
**Decided:**
- **One unit for every use of capital: an after-tax annual return, net of friction.** Friction is the round trip
  spread over the hold, the owner's hours at their rate as a share of the money in the asset (for picks, the money the
  sleeve manages on average over the hold), and the average month the money waits in T-bills while a minimum is saved,
  blended in at the T-bill rate after tax. "Ready in" counts a normal month's free pay (after the match and a twelfth
  of the HSA and IRA room) once the one-time steps — the emergency fund and debt above the hurdle — are done.
- **Each page's own estimate, never a second model:** Compounders' expected return (growth + buybacks + dividend ±
  multiple drift) for names passing its gates; the Aristocrats' yield + capped dividend growth for BUY/VALUE; Lynch's
  earnings yield + capped growth; Quiet Value's earnings yield; Schloss's discount to tangible book closing HALFWAY
  over the hold plus a yield capped at 8% (not below a third of book, where the market is saying the assets are not
  there). Each is blended toward the market return by a weight the owner sets (50% by default) and taxed as held in
  a taxable account.
- **BRRRR and flips are conditional.** /headroom removed the "typical fixer price" guess on purpose, so the board shows
  the owner's after-tax target, reached only at or below the solved price, with the share of the median it needs.
- **House hacks are owner-occupied**, so only ZIPs within the owner's radius of a home ZIP count; their return adds
  the rent the owner stops paying. **Buying a home** (NorCal screen) is the rent avoided less the full cost of owning,
  plus principal and the owner's appreciation assumption, on the cash to close — property tax and insurance from the
  same state tables /norcal uses (the owner engine invents neither, and returned nothing without them).
- **The month's waterfall**: starter cushion → the employer match → debt above the market hurdle (highest APR first)
  → the full emergency fund → HSA → Roth IRA (room spread over the months left in the year) → the top of the board. A
  lumpy winner is saved for in T-bills with the months it takes; a stock winner is split across the top picks, no name
  above its cap, and what the cap leaves is shown unassigned. The winner is never a debt the fixed steps already pay.
- **Private**: the page and the save endpoint are admin only; the profile is one JSON row in `capital_profile`.
  Contribution limits default to the IRS 2026 figures and the profile holds the room actually left.

## 2026-10-03 — /capital: now vs optimal, net worth steering real estate, house-hack leverage
**Why:** the owner asked for a current-allocation model on /capital, to compare where their money is now with the
optimal path. Owner choices: compare both what they hold and how they split each month's pay; move a holding only
if it pays after tax; one line per holding; owned real estate measured from its own numbers. They then asked how debt
and leverage are counted and said net worth should steer the path — real estate first for leverage and shelter,
more to stocks as net worth grows — and chose "math + a glide path": shelter counted once, lazy equity measured, and a
cap on real estate's share of net worth that tightens as net worth grows (no cap under $250k, 60% to $1M, 40% above,
all editable).
**Decided:**
- **One unit for holdings and pay lines, the board's**: an after-tax annual return net of friction. A ticker on a
  screen takes that screen's blended estimate; an index fund (a fixed list of US total-market and S&P 500 funds), a
  mutual-fund ticker, a fund named without a ticker, anything in a 401(k), and a stock no screen covers take the market
  return, labelled as assumed; cash takes the rate the owner gives and **cash without a rate is not measured** (no
  gap is claimed on it). Index funds are taxed with a 1.2% dividend (the S&P 500's yield, 2025-26).
- **A traditional balance counts at its after-tax value** (× (1 − the retirement rate)): the tax on the way out is
  proportional, so it earns its pre-tax rate on that value.
- **Owned property is measured on the equity that could be taken out** — value less 7% to sell, less the loan — so a
  paid-down or risen property shows its real ("lazy") return. Rentals: cash flow after the full payment, income tax on
  what depreciation (/headroom's 80% building share over 27.5 years) does not shelter, the year's principal, and
  appreciation after the gains tax. A home or a house hack you live in adds the rent you would otherwise pay, untaxed,
  and is never put up for sale. A loan without its payment is not measured.
- **The switch test**: a holding moves only if (1 − tax − costs)(1 + new return)^hold beats (1 + its return)^hold by
  at least 1% of the money moved. Taxable tax is the gain over the basis at the long-term rate (lots are assumed held
  over a year); a rental's is depreciation recapture at up to 25% federal plus state (plus NIIT if set) and the rest of
  the gain at the capital-gains rate. **Keeping is credited with never paying the deferred tax** (a step-up, or held
  for good) — the cautious reading, which tilts toward keeping. No basis (or no purchase price and year) → not tested.
- **Money moves only where its account allows**: a 401(k) to the plan's index fund; an IRA, Roth or HSA to the top
  picks or an index fund (no tax to switch; California's tax on HSA growth comes off); taxable money, free cash and
  rentals to anything on the board. **The emergency fund stays cash** — six months held in the best-paying cash first,
  movable only to T-bills. Destinations are filled best first; debt takes its balance; a property takes exactly one
  deal's cash or nothing; when the month's plan is saving for a property, free cash may join that down payment at the
  property's return. A holding already among the top picks stays.
- **Pay vs the waterfall**: the owner's split, line by line, against the waterfall's split of the SAME amount. A new
  401(k) dollar is deducted now and taxed later, with the employer match on the first matched dollars; a Roth dollar
  grows untaxed; an HSA dollar is deducted (federally only in CA/NJ). Dollars are the year's return on ONE month's
  money, because this month's waterfall can hold one-time steps; a gap is claimed only when every line is measured.
- **One source of truth**: once holdings are listed, their cash lines ARE cash on hand and their individual stocks the
  picks held (both read-only on the form). Net worth = cash + everything invested + property equity − debts.
- **Shelter is counted once**: owning where you live (a home or a house hack) zeroes the rent you pay, takes the rent
  credit off every house hack, and drops "buy your home" from the board. BRRRR and flips were already on investor
  terms; a further house hack would mean moving, so it keeps owner financing but loses the rent credit.
- **The glide path**: real estate's share of net worth may not exceed the cap at the net worth the owner will have
  when a property is funded (today's plus the free pay saved by then; returns not counted, so it errs low). A
  property that would take it over is blocked — ranked below everything buyable, never the month's winner, never a
  destination for held money. Property already over the cap is not force-sold; new money goes elsewhere and selling
  is only suggested where it pays after tax.
- **House hacks now count leverage fully**: the year's principal on /headroom's FHA loan (3.5% down on price + remodel,
  the upfront MIP financed) and the owner's appreciation setting on the all-in price, as "buy your home" already did.
  At 7.28% a $700k house hack pays about $6,600 of principal in year one — about 14.5 points on $45,500 to close.
- Saved as typed: the holdings and pay-split boxes keep the owner's lines, so a line the engine cannot read stays in
  the box and is named on the page. No migration — the profile is one JSON row.

## 2026-10-03 — /capital Do next: each step's return in percent
**Why:** the owner asked to see each Do-next step's % return — the immediate return of that decision — not only
dollars a year.
**Decided:** every step shows what the money earns a year where it is → where it goes, after tax, and the gap in
points; a step selling several holdings weights the return now by what is sold of each, and the return after by what
arrives (it had shown the first holding's alone). Under it, what the rate is: a debt's APR saved until pay would clear
it (or for as long as it runs), a property's return over the hold with year one beside it, the top picks' average
after tax and research time, T-bills after federal tax. When moving costs tax or trading, its share of the money and
the months the gain takes to earn it back. The monthly switch shows the split now → the plan's, in percent of a year's
contributions. "Why this" lists each holding's own return.

## 2026-10-03 — /capital: live holding values
**Why:** the owner asked whether /capital changes as numbers and prices change, and for it to be a real-time tool.
It recomputed on every load, but holdings were frozen at the last export's dollars. Owner choice: live holding
values first (daily snapshots and alerts, transfers to set up, and broker connections were offered and not chosen).
**Decided:**
- **A share count on a holdings line** (`…, 40 sh`) lets it be valued at shares × the live price. Synced lines carry
  the export's share count; a typed line can have one (the editor's Shares column). Lines without one keep their
  dollars. Cash, debts and property are unchanged — they move when re-imported, edited or marked done.
- **Repriced before anything reads them** (`capital.build` → `allocation.reprice`): net worth, the board, Do next,
  What if and the monthly plan all use today's values. Mark done and Apply edit the repriced lines, so the values
  they write are the ones the page showed. A pick bought that way gets its share count from the live price (the
  board's top picks are priced on every live build), so it stays live.
- **One batch, cached, time-boxed** (`stock_lookup.live_prices`): every priced ticker in parallel through the
  existing Yahoo-then-Nasdaq lookup; 15 minutes in memory while the US market is open, an hour otherwise; the page
  waits at most 6 seconds. A ticker not back keeps its last value and is counted as stale. "Refresh" on the
  overview (`/capital?fresh=1`) goes past the cache. Test builds pass prices and never fetch.
- **On the page**: under net worth, "Live · prices 3:42 PM ET · today +$X" (or "Prices unavailable · values as last
  synced"), with Refresh; on Holdings, each priced line's shares × price and today's move.
- **A Do-next step's id is what moves where, not the dollar amount** — with live prices the amount drifts between
  loading the page and pressing Mark done, which had turned a step into "no longer on the list".

## 2026-10-03 — /capital Monthly plan, month by month; property and debt returns over the hold
**Why:** the owner asked for a world-class Monthly plan. Their screenshots showed "a normal month from January" putting
most of the pay into a down payment forever, an optimal path at 55% a year and a year-15 gain near a billion dollars.
Owner choices:
annualize property returns over the hold; a month-by-month timeline; build directly, reviewed on the PR.
**Decided:**
- **Property returns are annualized over the hold** (`capital.hold_return`), for the board's house-hack and
  buy-a-home rows: the cash to close becomes equity at a sale in `hold_years` (the price grown at the owner's
  appreciation, the loan paid down, 7% to sell), plus each year's benefit (rent saved and net rent after the full
  payment) reinvested at the after-tax market return. Leverage counts in full; year one's return on the cash (over
  80% for a typical FHA house hack) is no longer compounded — it is shown in the row's basis. The sale's tax is not
  charged (the home-sale exclusion) and the yearly benefit is held flat. BRRRR/flip rows are already a target
  return; owned property keeps its return on equity (BACKLOG).
- **One deal at a time**: the optimizer funds at most one property — the month's plan's own when it is saving for
  one, else the best — and the down-payment fund is that same deal. Three house hacks funded at once had produced the
  55% path.
- **A debt paid early saves only the months until pay would clear it** (`capital.debt_hold_rate`): the APR for those
  months, then the after-tax market return, annualized over the hold. The timeline says when pay alone clears each
  debt; a debt it never clears keeps its full APR. So Do next no longer sells holdings to pay a card next month's pay
  clears, What if says "your pay clears it by Nov anyway", and the overview's year-15 figure is no longer a card
  compounding at 23% for fifteen years. Debt above the market return still comes first in the monthly waterfall.
- **The timeline** (`timeline.py`) runs the monthly amount through the board's own waterfall month by month,
  carrying the state: cushion and emergency fund; each card's monthly interest at its APR, then the plan's payment; a
  loan's listed payment comes from expenses and joins the monthly amount once it is paid off; Roth/HSA/401(k) room
  drawn down and reset each January at the 2026 limits; a down payment started by cash above the emergency fund,
  filled exactly, then — after the months to close — rent stops and the place's own cash flow starts. Steady is the
  first run of identical months that each began with every one-time goal met; it is shown as a January (a normal
  year's room). Pay alone: selling holdings is Do next, and the plan tab says when Do next would buy the deal today.
- **The tab**: the monthly amount now, when the one-time goals are done, the steady month (with what it frees); dated
  milestones; a chart of where each month's money goes (pick a month — keyboard too — for its split and events);
  the steady split; the owner's split now, or a way to add it. Nothing in the timeline grows at a return.
- The overview's year-15 figure on an owner-shaped test profile went from absurd to the house hack's modelled
  value: equity at the sale plus fifteen years of rent savings reinvested. The monthly plan shows the same rent
  savings as cash freed after move-in — two views of one deal, never added together.

## 2026-10-03 — /capital positions import: Chase, a line-by-line check, and naming an account the file does not
**Why:** the owner sent Chase's (J.P. Morgan Self-Directed Investing) positions export. It names no account at all,
calls its columns Ticker / Value / Cost, and has no total and no "% of account" to check against.
**Decided:**
- **Read by its columns** like the others (Ticker, Value, Cost, Quantity, Price, Asset Class, As of); Chase is known
  by its "J.P. Morgan Securities" footnotes, after the Schwab title, the file name and Fidelity's/Vanguard's columns
  (a holding named JPMorgan Chase does not make a file Chase's). Checked against the owner's real export, read locally.
- **A third check when there is no total**: every line's value must be its quantity × price (to 0.2%) — it catches a
  misread column, which is what the total checks catch elsewhere. A line that fails is named.
- **A margin account is taxable**: negative cash, or positions held in the Margin lot type, settle an account the
  file does not name — retirement accounts cannot borrow. A cash-only account with no name is still asked about.
- **Naming**: an account the file does not name gets a name field in the preview (default "Account"); the group's
  key and its margin debt follow the name, so the same name next time replaces the same group. The preview suggests
  the name the broker's unnamed group was last synced under, and says whether the sync replaces a group or starts
  one. Two accounts given one name are refused.
- The profile form's grid now holds its column to the page width — a synced group's five-column table had pushed
  the whole page 133px wider than a phone.

## 2026-10-03 — /capital: import a broker's positions export and keep it in sync
**Why:** the owner exported Schwab's Positions page and asked for it to sync to /capital and fill in the gap — the
holdings should come from the broker, not be retyped, and whatever the export does not cover stays as typed.
**Decided:**
- **A file, not a login.** The owner downloads the Positions CSV and picks it on the holdings step; no broker
  credentials are asked for or stored. Schwab's one-account and all-accounts exports are read by their layout (title
  line, a label line per account, "Positions Total" / "Account Total"); Fidelity's and Vanguard's by their column
  names, whatever view they were downloaded from (Account Number / Name, Symbol, Current Value / Total Value, Cost
  Basis Total when the view has it). Schwab's and Fidelity's layouts were checked against the owner's real exports
  (read locally, never committed); Vanguard's follows its published columns and is tested on a made-up file.
- **Preview, then sync.** Nothing is saved until the owner presses Sync. The preview shows each account, the kind of
  account it was read as (changeable; an account whose name does not say is flagged), every position, and a **check
  against the broker's own total** — positions + cash − margin + what was left out must match it to the dollar, or
  the gap is shown. Fidelity has no total row; its "% of account" column implies one (the largest line's value ÷ its
  percentage, good to that percentage's rounding), and pending activity counts toward it as Fidelity counts it.
- **A brokerage window** (Fidelity BrokerageLink, Schwab PCRA) is inside a 401(k) but does not say pre-tax or Roth:
  it is read as a 401(k) and flagged for the owner to choose. The board still measures every 401(k) line as a plan
  fund at the market return, and the preview says so (CAPITAL-WINDOW). The server reads the file again on Sync rather than trusting the page.
- **Each account is one block** in the holdings, between `# sync <broker>-<last digits>` and `# end sync` markers.
  A later export of the same account replaces its block where it sits; typed lines (checking, the 401(k), a fund at
  another broker) are never touched. Typed lines for the same ticker in the same kind of account are offered for
  removal, ticked, so nothing counts twice; other typed lines in that kind of account are listed, unticked.
- **What is written**: the market value; the cost basis in a taxable account only (where the switch test taxes a
  sale); a money-market fund at the board's T-bill rate (money funds track it) — as cash in a taxable account, inside
  the account otherwise; uninvested sweep cash at 0% (the owner can type what it pays); **negative cash is a margin
  loan** — a debt named "<broker> margin …<digits>" at the APR the owner gives (required), updated or removed by the
  next sync; it has no schedule, so no monthly payment is asked for. Options, bonds, CDs and pending activity are
  named with their value and left out.
- **Synced lines are shown, not edited**: the row editor holds only the typed lines; a synced group can be turned
  into typed lines ("Edit by hand") or removed, and a save writes each group back exactly as it was. The holdings
  step says what is synced and when, and asks for a fresh export after 30 days.
- **Privacy**: only the last digits of an account number are kept, as the broker shows them. The owner's export is
  not in the repository; the suite's files are made up in each broker's layout. Sync keeps the one-level undo.

## 2026-10-03 — /capital: ticker search and a live price where a ticker is typed
**Why:** the owner typed a fund's ticker into the holdings editor and asked to search tickers and pull in real-time data. Owner
choice: search plus the live price (not live values for every holding).
**Decided:**
- **One type-ahead** for every box that takes a ticker: holding and monthly-pay names (not cash, T-bill or debt rows)
  and the What-if ticker. It lists up to 8 US stocks, ETFs and mutual funds (exact ticker first), works with the
  keyboard (arrows, Enter, Escape) as an ARIA listbox, and keeps whatever was typed when nothing is picked — a name
  like "Stable value fund" is still a valid line.
- **Sources in order**: Yahoo's search (with the plain named agent Yahoo answers from cloud IPs), then Nasdaq's
  autocomplete, then the SEC's ticker list as a floor (operating companies only). Foreign lines (Yahoo writes them
  with a dot suffix — SAP.DE, VOD.L — and a US share class with a dash, BRK-B), indices and crypto are dropped. A
  found answer is cached for the day; an empty one is not (the next try may reach a source). Text beyond letters,
  digits, spaces and . & ' - is refused before any request.
- **The price** under the box: last price and today's change from Yahoo's chart (five days, not a year), else
  Nasdaq's quote tried as a stock, ETF, then mutual fund — and which screen rates the ticker (one of the top picks,
  rated by a screen, or on no screen so the market return is assumed). It is shown, not written into the line: a
  holding's value is still the dollars the owner enters.
- **Not covered**: no request leaves the sandbox, so the suites test each source from a recorded response shape and
  the browser check uses stubbed answers; the live endpoints are first exercised on Railway.

## 2026-10-03 — /capital What if: weigh a move before making it
**Why:** the owner asked for a calculator that thinks through a potential move against the current allocation and
says whether it pays or is suboptimal. Owner choices: shifting money first (property moves later); the verdict
against doing nothing AND against the best use of the same money; save and compare scenarios and apply one; its own
"What if" tab with a way in from Do next.
**Decided:**
- **A move** takes an amount (or all) from one holding into the top picks, a ticker, an index fund, T-bills, paying
  down a debt, or something else at a pre-tax return the owner gives. Account rules hold: an IRA, Roth or HSA move
  stays inside the account (no T-bills, no debt payoff — that would be a withdrawal); a ticker or the picks inside a
  401(k) is allowed but flagged (only through a brokerage window).
- **The verdict is the board's arithmetic**: keep = amount × (1 + h)^hold against move = amount × (1 − costs − tax) ×
  (1 + a)^hold, in after-tax dollars (traditional at after-tax value), where a is the board's after-tax return net of
  friction for new money in that account: a ticker on a screen at its own row's net, one on no screen at the market
  return less its round trip and research time, the picks at their average, an index fund at the market with a 1.2%
  dividend, something else as growth taxed at the end. Better / worse needs a 1%-of-the-money gap over the hold — the
  Do-next bar; anything closer is "about the same". It also gives the years to earn back the tax and costs.
- **Against the best use**: the board's destinations for that account filled best first (a debt takes at most its
  balance; individual tickers are not on the list — the board's answer for stocks is the top picks); if keeping beats
  them all, keeping is the best use. The page says what the move gives up against it by the end of the hold.
- **Warnings, not silence**: an amount above what is there (capped), above the debt (capped at the balance), cash
  below the emergency fund, one name above the per-name cap, a missing cost basis (tax not counted — may look better
  than it is), a move into the same shares, and a sale at a loss (what it saves this year, shown apart from the
  verdict, with the wash-sale rule).
- **Live in the browser, checked against the server**: the page gets the engine's numbers (a "kit") and runs the same
  arithmetic in JavaScript as it is typed; saved scenarios are weighed by the Python engine, and a browser check loads
  each saved one into the live calculator and requires the same dollars.
- **Saved scenarios** live in the profile (name, source by holding name and account — lines move — amount or all,
  destination), are weighed again on every load, say so when their holding is gone, and can be applied: the move is
  re-checked against the saved profile on the server and carried out the way Mark done carries out a step, with the
  same one-level undo.

## 2026-10-03 — /capital as a dashboard: Do next, marking steps done, and the questions an individual asks
**Why:** the owner asked for the dashboard to be the best UI it can be and for what else an individual would want to
see and calculate. They chose: an Overview with a ranked Do-next list first; per-property numbers with a refinance
test, tax helpers, independence and debt payoff, passive income and liquidity; and a clickable mockup before the
build. They approved the mockup ("build it").
**Decided:**
- **One page, seven tabs**: Overview (net worth and its mix, return now → optimal, the gap; four vitals — cash runway,
  saving rate, real estate's share against the cap, progress to independence; Do next; a rail with independence,
  debts, passive income, liquidity), Monthly plan, Holdings, Property, Taxes, Board, Profile. Market Pulse's own
  tokens and type.
- **Do next is one ranked list** of the held-money moves (grouped: the emergency fund into T-bills is one step, a
  property sold is one step whatever its money funds) and the monthly split, ranked by after-tax dollars a year —
  debt above the market return first, because its return is guaranteed. The one-time steps add up to the headline gap.
- **Mark done edits the profile** as if the move were made: the source line sold down (its basis in proportion) or
  removed, a debt paid down or removed, the new holdings appended (the picks one line each, split as the waterfall
  splits them; what a per-name cap leaves to T-bills or the account's index fund), or the monthly split rewritten.
  The owner's notes and unreadable lines are left exactly as typed. The profile before the step is kept for ONE
  undo. Steps are found by a stable id; a step no longer on the list is refused (409), never guessed. Buying a
  property is not marked done for the owner — its price, loan and rent are theirs to enter.
- **No step is suggested twice**: a property is sold whole or not at all (only when the money left after a smaller
  destination still beats keeping it in the picks); cash already in T-bills is not "moved" into a down-payment fund;
  a payment to a debt that is paid off or being cleared counts as freed cash at the T-bill rate. Every step, applied
  and rebuilt, is gone from the next list (tested for each).
- **"Top picks"** in a holdings or pay line means the board's top picks as one equal-weighted group at their average
  estimate, so a rewritten monthly split reads back at exactly the waterfall's returns.
- **A normal month** (for the monthly step and the plan tab) is the waterfall from January with the one-time steps
  done and a full year of 401(k), IRA and HSA room, on the same total as the owner's split.
- **Independence**: a year of expenses × the multiple (25 = a 4% withdrawal), net worth less the home you live in,
  compounding monthly at each path's after-tax return less inflation plus the monthly saving; Coast FI at the
  retirement age needs the owner's age (new, optional).
- **Debts** take an optional monthly payment (a fourth field) for the payoff month and interest left; $100 more a
  month is weighed against the best use for new money (the top picks as a group when picks win).
- **Property**: return on the equity that could be taken out, debt coverage, cap rate, loan-to-value, tax if sold;
  for a rental, a keep / cash-out refinance / second loan / sell calculator over the hold, with the lender's
  loan-to-value and DSCR limits (defaults: 75%, 1.20×; refinance at today's rate + 0.5 for an investment property, a
  second loan at + 1.25; closing 2% and 1%). Cash taken out earns the top picks' after-tax return, and the page says
  plainly that this borrows to buy stocks.
- **Taxes**: the rates in use (still typed — bracket tables are BACKLOG), taxable holdings below cost with the tax a
  loss saves ($3,000 a year against pay, the rest carried) and the wash-sale rule, a Roth-vs-traditional slider on the
  retirement rate, and the Roth IRA income test (salary less traditional 401(k) contributions) against the IRS 2026
  phase-outs — single $153,000–$168,000, married filing jointly $242,000–$252,000 (Notice 2025-67, as the limits).
- **Profile in five steps** (Pay and taxes with the monthly split, What you hold, Property you own, Debts, You and your
  assumptions), each saved on its own, with row editors instead of text boxes. New fields: age, retirement age,
  filing status, take-home pay, the independence multiple, inflation.

## 2026-10-03 — Email sign-in links: a way in that Google cannot block
**Why:** the owner signing in with their personal Gmail was stopped by Google itself — "Error 403: org_internal",
because the Google OAuth app is a Workspace-internal app — before this server was asked. No code here can change
that check; it is a setting in the Google Cloud project. The owner asked to be let in and never be blocked.
**Decided:**
- **A one-time link, mailed to an address on ADMIN_EMAILS / SALES_EMAILS, signs it in** with the same 30-day
  session Google sign-in sets. Sent through Resend, which the CRM already uses. The link is signed under its own
  context (it can never pass as a session cookie, nor a session cookie as a link), expires in 15 minutes and works
  once — recorded under a lock so two racing clicks cannot both sign in.
- **The link opens a confirm button, not the sign-in**, because mail scanners open links on their own and would
  use it up.
- **The answer never reveals who is on the list**: every address is told to check its email; only listed ones get
  a link. At most 3 links per address and 10 per IP per 15 minutes.
- **The link points only at this site**: PUBLIC_BASE_URL when set, otherwise the Host the request arrived on —
  never X-Forwarded-Host, which a caller could set to have a genuine link mailed out pointing at their own server.
- The Google callback's post-login redirect now uses `_safe_redirect` ("//elsewhere" starts with a slash too), and
  /capital sends a signed-out visitor to /sign-in, which offers Google, the email link and the admin token.
- Still the owner's to do: their Gmail must be on ADMIN_EMAILS (Railway). Making the Google app External in Google
  Cloud would let Google sign-in work for it as well; the app still refuses any address not on the lists.

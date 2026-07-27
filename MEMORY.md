# MEMORY.md
Decision log for ava_dashboard. Read this at the start of every session.

---

## 2026-07-27 — Card tracker: Pokémon prices move to PPT, resolved free off the catalog

**Decided:** The nightly ingest is split by game. **Pokémon** prices come from
PokemonPriceTracker (`run_ppt_ingest`); **One Piece** stays on JustTCG
(`run_ingest`, now filtered to `game = 'one_piece'`). One raw-price snapshot
per card per day, into `price_snapshots` as before.

**Resolution is now free, and that's the main win.** `catalog_cards` already
holds `tcgplayer_id` for every card in a stocked set, so `resolve_tcgplayer_ids`
matches tracked cards against **local rows** — replacing JustTCG's
search-per-card pass, which was the ingest's most expensive step (55-call
budget, paced at 10/min) *and* its main source of wrong matches. Pricing is
then pinned by `tcgPlayerId`, so there is no name/set matching at request time
at all. `fetch_ppt_card_prices` deliberately omits `includeEbay`: that flag
bills a second credit per card for graded data the tracker doesn't store, so
this is 1 credit per card.

**Side effect worth knowing:** One Piece now has the entire JustTCG budget to
itself instead of competing with several hundred Pokémon cards.

**THE TRAP — scoring must never span two sources.** `card_scoring` computes
momentum from `price_mid`. JustTCG's "mid" and PPT's "market" are different
estimators of the same thing, so a naive switch makes momentum report the
change of ruler as a price move. Measured on synthetic data: **31.0% vs 0.77%**
for the same card. `card_scoring.select_scoring_series()` groups snapshots by
source family and scores only one — preferring the newest family once it has
≥2 points, falling back to the largest otherwise. `justtcg` and
`justtcg-history` are one family (same estimator); `pokemonpricetracker` is
its own. Old snapshots stay in the DB and still draw on the graph; they just
don't feed momentum. **If you add a third price source, add it to
`SOURCE_FAMILIES` or momentum will silently break again.**

**Unresolvable Pokémon cards get no price, deliberately.** A card whose set
isn't stocked yet is reported, not fetched from JustTCG as a fallback — that
fallback is exactly what would mix two estimators into one series. The fix is
to stock its set on `/catalog`, after which it resolves on the next run.

**`match_catalog_row` refuses to guess.** A *disagreeing* card number is
disqualifying, not merely lower-scoring: a set holds several printings of the
same name, and picking the wrong one would track the wrong card's price for
months without any visible symptom. Ambiguous matches return None.

**Budget:** `TRACKER_PPT_RUN_CAP` (default 150) bounds credits per run, and a
429 backs off then stops cleanly, same rule as the catalog backfill. PPT's
budget is shared with the catalog and the grading calculator and **has already
been seen 429ing**, so this takes a bounded slice rather than assuming it's
free. Watch `ppt_credits` in the refresh summary.

**Unverified:** never run against the live DB or a real PPT key. The matcher
and the series selector are unit-tested; the ingest path is not.

---

## 2026-07-26 — Card catalog: local cache filled free off the grading calculator

**Decided:** New `/catalog` page — browse/filter every card the dashboard has
seen by set, rarity and raw market price. Backed by a new dashboard-owned
table `catalog_cards` (`catalog.py`, `CREATE TABLE IF NOT EXISTS` at startup,
same convention as the card tracker). Reads never touch a vendor.

**The finding that shaped the whole design:** PPT's `/cards` response already
carries `prices.market` and `rarity` for **every** card, and
`price_sources.fetch_ppt_set_cards` was already paying for that call (billed
per card) to drive the grading calculator's picker — then discarding the
price. `fetch_pokemonpricetracker` reads raw from exactly that field. So a
whole set's raw prices cost **zero extra credits**; they were being thrown
away. `_ppt_raw_price()` was added and the price threaded onto the existing
card dicts (additive — the picker ignores the new key).

**Filling is lazy, never member-triggered.** `/api/grading-calculator/set-cards`
upserts its result into `catalog_cards` on the way past, so browsing the
calculator stocks the catalog for free. Plus an admin-only `POST
/api/catalog/stock` for seeding sets deliberately. **A member browsing
/catalog can never cause a vendor call** — `/api/catalog/cards` is a pure DB
read. That's the point: "every holo under $50 across all sets" spans ~36,000
cards, and at 1 credit/card a full upfront backfill is unaffordable. The
catalog stocks itself from real usage instead.

**Seeded with the newest 20 sets** (`CATALOG_BACKFILL_SETS`, env-tunable, 0
disables) by a background task at startup, so the page isn't empty on day one.
Everything older arrives lazily.

**The seed slices BEFORE filtering out already-stocked sets, and that order is
load-bearing.** `catalog.select_backfill_window()` takes the newest N *then*
drops what's cached. The obvious alternative — "the next N sets nobody has
stocked" — walks further back through the catalog on every call, and since
Railway redeploys on every push and the in-process PPT cache dies with the
process, that would re-spend ~4,000 credits on **every deploy, forever**.
Slicing first pins a fixed window: first boot pays once, every later boot is a
no-op, and a newly released set enters the window on its own. This is covered
by tests (the redeploy no-op, the new-release case, and that a set pushed out
of the bottom of the window is never re-fetched) — if you change that
function, keep those properties.

**The seed stops after 3 consecutive FAILED REQUESTS — not 3 empty sets.**
The first version aborted on empties, and it tripped immediately in
production against a perfectly healthy API. The newest sets are exactly the
ones PPT most often has no data for yet, so "the newest 20" is the window
most likely to contain empty sets — the abort condition and the window
selection were in direct conflict.

`_ppt_get` already knew the difference; `fetch_ppt_set_cards` was throwing it
away. Added `fetch_ppt_set_cards_detailed()` returning `(cards, status)`,
with `fetch_ppt_set_cards` kept as a thin list-returning wrapper so the
pickers are untouched. Empty sets are now skipped and the run continues; only
real request failures count toward the abort.

**Then production showed a THIRD outcome: HTTP 429.** The first fix
classified it as `error`, which was still wrong — a 429 means "later", not
"never". `_ppt_get` now returns the response on a non-200 as well (it was
returning `None`, which erased the status code), so status is `ok` / `empty` /
`error` / **`rate_limited`**:

- `rate_limited` — retried in place with a doubling wait
  (`CATALOG_BACKFILL_RATE_WAIT_S`, default 30s, 2 retries). Still throttled
  after that stops the run **cleanly**; the button resumes from where it
  stopped, and already-stocked sets are skipped, so nothing is paid twice.
- 429s log `Retry-After`, `X-RateLimit-Daily-Remaining` and
  `X-API-Calls-Consumed`. **That's the line to read**: it's what separates a
  short per-minute throttle (waiting fixes it) from the daily cap being spent
  (nothing helps until it resets).

A rate-limited set is never cached, so the retry genuinely re-requests it.

**Empty sets are never cached and never stocked, which makes this
self-healing.** They stay outside `catalog_cards`, so they remain in the
newest-N window and get retried on the next boot — and an empty result costs
no per-card credits, so those retries are nearly free. A set stocks itself
whenever PPT finally publishes it, with no intervention.

**Two selection functions, and they must not be swapped.**
`select_backfill_window` (newest N, slice-then-filter) is the *automatic*
startup path and is idempotent across redeploys. `select_next_unstocked`
(walk the whole catalog, no window) backs the admin **"Stock 5 more sets"**
button — each press takes the next 5 uncached sets and marches further back
into older ones. That rolling behaviour is exactly what makes it unsafe for
startup and correct for a button: one press, one bounded batch, a person
choosing to spend the credits. Both are covered by tests that assert the
distinction.

**The button needs `_catalog_known_empty` or it stalls.** Since empty sets
are never written to `catalog_cards`, "the next 5 unstocked" would return the
same empty sets on every press and never advance. Empties are remembered
in-process and excluded. Deliberately NOT persisted: a redeploy clears it, so
a set that had no data last week gets another try — which is what should
happen once PPT publishes it. There's a test that removes the skip set purely
to prove the stall is real.

**Rejected:** a full nightly sync of all ~180 sets (blows the credit budget —
verify the actual quota in the `X-RateLimit-Daily-Remaining` log line before
anyone revisits this); on-demand fetching from the catalog page (a member
typing in a price filter would spend credits); client-side filtering (can't
answer cross-set questions without holding every set in the browser).

**Pokémon-only on purpose.** One Piece's catalog source (optcgapi) returns
rarity but **no prices at all**, so OP cards would sit in a price-filtered
table with nothing to filter on. `CATALOG_GAME = "pokemon"` in `main.py`; the
table's `game` CHECK still permits `one_piece` so adding it later is a code
change, not a migration.

**Two schema details that will look wrong if you skim them:**
- `rarity` is part of the unique index. One Piece prints alt-art/manga
  variants under the *same name and number* — without rarity in the key those
  collapse into one row and a variant silently vanishes. Costs nothing for
  Pokémon; correct for when OP is wired up.
- A refresh takes the new response as truth: a card that no longer reports a
  price has its cached price **cleared**, not left to look current.
  `priced_at` is set only when a price is actually stored, so it always
  describes the number next to it.

**Freshness is honest, not live.** Prices are captured when a set was last
stocked. The page says so, and a blank price renders as "no price", never
`$0` — those mean different things and conflating them would misprice a card.

**Card names link out, and the link gate fails closed.** TCGplayer when the
id is trustworthy, otherwise an eBay *search* URL — so every row is clickable
and none can point at the wrong product. Both are built from data already in
the table: no API call, no credits, and no CSP change (`img-src` doesn't
govern `<a href>`, and there's no `form-action` rule).

The trap: `catalog_cards.tcgplayer_id` was NOT reliably a TCGplayer id.
`fetch_ppt_set_cards` populated it via
`_first(row, "tcgPlayerId", "tcgplayerId", "id")` — falling back to **PPT's
own id**, which points at a different product entirely. A naive
`tcgplayer.com/product/<id>` link would have sent people to buy the wrong
card. Fixed at the source with an additive `tcgplayer_id_verified` flag
(true only when the value came from a real TCGplayer field); the `id`
fallback is deliberately preserved because the price lookup already depends
on it. `catalog.tcgplayer_url()` requires the flag **and** a numeric id, and
returns None otherwise. eBay is a search, not a listing: listing ids go stale
as items sell, and resolving live ones costs an API call per card, which a
50-row page can't afford.

`tcgplayer_verified` shipped after the table existed, so it needs the
`ALTER TABLE ... ADD COLUMN IF NOT EXISTS` in `CATALOG_SCHEMA` —
`CREATE TABLE IF NOT EXISTS` will not add a column to a live table. Rows
cached before it default to FALSE and simply get the eBay fallback until
they're re-stocked, which is the safe direction.

**Images were considered and deferred.** CSP already allows
`https://images.pokemontcg.io`, so that source needs no security change — but
catalog rows come from PPT (set ids are *names*) while pokemontcg.io needs
set *codes* (`swsh12pt5`), the same id-space mismatch behind the
set-cards fallback bug. `grading_sets.py` holds real pokemontcg.io ids and
could bridge both. TCGplayer's image CDN would need an `img-src` addition.
Worth checking the existing `set-cards first row keys` log line first: if PPT
returns an image URL directly, images become as cheap as links.

**Security:** `ORDER BY` resolves through the `SORT_COLUMNS` allowlist
(unknown keys fall back to the default) — same rule as the raffle wheel's
`_pick_col`. Everything else is parameterized, including ILIKE patterns,
whose `%`/`_` are escaped so typed text can't become a wildcard. Page and
read APIs are member-gated; `/api/catalog/stock` is `require_admin` because
it spends credits.

**Nav:** this is an **11th** template carrying the copy-pasted nav dropdown
(see the 2026-07-23 entry). The `Card Catalog` link was added to all 10
existing templates in the same change, per that entry's rule. Extracting a
shared base template is still the real fix and still wasn't attempted.

**Cross-repo:** additive only — one new dashboard-owned table, no bot-owned
table touched. Log in data-system.md's ownership table.

---

## 2026-07-23 — Nav: "Admin Tools" dropdown, click-to-open, duplicated per template

**Decided:** The four admin-only nav links collapse into one `Admin Tools`
dropdown holding **Visitor Log** (renamed from "Admin"), **Contributors**, and
**Network**. Raffle stays a top-level button. Opens on **click**, closes on
outside-click or Esc (`toggleNavGroup` / `closeNavGroups`, ~15 lines appended
before `</body>` in each template).

**Why click, not hover:** hover doesn't exist on touch, the nav already wraps on
phones, and a hover menu sitting next to "Log out" pops open on the way past.
Rejected: pure-CSS `:hover` (cleanest diff, unusable on mobile) and native
`<details>` (free a11y, but fiddly popup positioning and browser-inconsistent).

**Duplicated across 10 templates on purpose.** There is no Jinja base template
in this repo — every page is standalone with its own inlined `<style>`. The
dropdown CSS/markup/JS is therefore copy-pasted into all 10. **If you change the
dropdown, change it in all 10** (`status, card_tracker, map, contributors,
raffle_wheel, invite_network, scan, analytics, admin, index`). Extracting a
shared base template is the real fix; not attempted here.

**Three template quirks that will look like bugs in the diff:**
- `admin.html` and `analytics.html` do **not** wrap the group in
  `{% if is_admin %}` — their routes (`main.py`) never pass `is_admin`, so a
  guard would hide the menu on the pages it belongs to. Matches how their nav
  links were already hardcoded.
- `invite_network.html` has an `{% else %}` branch with a standalone Network
  link: `/invite-network` is viewable by non-admin `all_mods` users, who would
  otherwise lose the link by it moving inside an admin-only group.
- `scan.html` is dark-by-default with a `html[data-theme="light"]` override —
  inverted from every other template, so it needs its own colour block.
- `index.html` uses `.logout`-classed links in `.header-right`, not `.nav`.

**Mobile:** menu is `right: 0` anchored (nav sits at the right edge on desktop);
each template's media query flips it to `left: 0` so a wrapped nav row doesn't
push the 150px panel off-screen left.

---

## 2026-07-23 — Card tracker: detail panel above the table, 7/14/30/90-day graph range

**Decided:** The card detail panel moved above the main table (was below), and
the price graph got a 7/14/30/90-day range toggle defaulting to 30.

**90 days is pursued two ways, on the assumption neither is reliable alone.**

*Ask the source for more.* JustTCG's `priceHistoryDuration` accepts
7d/30d/90d/180d/1y and **defaults to 7d** — so a request that fails to carry the
parameter silently returns a week, which is what we were seeing. Fixed in
`justtcg.py`: duration is now `HISTORY_DURATION` (env `JUSTTCG_HISTORY_DURATION`,
default `90d`) and history is requested **three ways at once** —
`priceHistoryDuration` and `include_price_history` on the query string, plus
`include_price_history: true` on each batch body item (the field their SDK
documents on `BatchLookupItem`). The query string alone appears to be ignored on
the batch POST. Unverified against a live key — `_log_history_depth()` logs the
returned point count every run precisely so a regression to ~7 is visible
instead of silent.

*Accumulate regardless.* **Nothing has ever deleted snapshots** — the only
`DELETE FROM price_snapshots` statements are the two admin buttons — so each
nightly ingest adds a day and the window fills in over ~3 months even if the
source only ever hands back a week. Deliberately did NOT add a retention/prune
job; rows older than 90 days are kept.

**Backfill now runs every ingest, not just on a card's first fetch.** It inserts
only calendar days (UTC) the card doesn't already have, so it is idempotent
without a unique index on (card_id, day) — and it self-heals two things the old
first-fetch-only version could not: nights the scheduler didn't fire, and cards
permanently stuck on a 7-day history from before this fix. Costs no extra API
calls (same requests, bigger payload). Rejected adding a unique constraint:
existing rows may already contain same-day duplicates from manual refreshes, so
the migration could fail on live data.

**Graph collapses to one point per calendar day** (`dailySeries`). A manual
"Refresh prices" can add several snapshots to a day on top of the scheduled
nightly one, which would otherwise show as a jagged multi-point day. Last
snapshot of a day wins.

**Range windows step by calendar days, not `n * 86400000`.** A fixed 24h step
drops a point from the window across a DST change — caught by a test, fixed with
`setDate()`. Same trap applies to any future date-window code here.

**Gaps are surfaced, not hidden.** The note under the graph counts days in the
span with no price ("5 days in that stretch have no price yet") so a nightly
ingest that stopped landing is visible instead of looking like a smooth line.
The scheduler is an in-process asyncio loop (`main.py`), so a Railway restart
near 11pm silently skips that day. No startup catch-up was added on purpose —
Railway redeploys on every push and each catch-up would spend JustTCG's 100/day
free-tier budget.

**Range filtering is client-side.** `/api/card-tracker/history` already returns
every snapshot for a card in one response, so switching range trims the array
already in memory — no extra request, no JustTCG budget. Rejected adding a
`days` query param: it would spend a round-trip per toggle for data already
sent.

**The window is anchored to the card's newest snapshot, not to `now`.** A card
whose prices haven't refreshed in weeks still draws a line on the 7-day view
instead of going blank. Trade-off: the "last 7 days" label is relative to the
data, not the calendar — the note under the graph says which.

**Score and momentum chips still use full history** regardless of range; they're
computed server-side in `card_scoring.score_card`. The explainer text under the
graph says so explicitly, so nobody reads a 7-day graph and expects the 30d
momentum number to track it.

---

## 2026-07-11 — Card tracker (trial): dashboard-owned tables, startup-ensure DDL

**Decided:** The card profit-potential tracker's three tables (`tracked_cards`,
`price_snapshots`, `card_scores`) are **dashboard-owned** and created via
`CREATE TABLE IF NOT EXISTS` at app startup (`card_tracker.py`, called from the
lifespan) and by the ingest script. Watchlist lives in `watchlist.py` (config,
not DB). Page access reuses the existing `MOD_ROLE_IDS` session-mod flag +
admins — regional mods get access by adding their role ID to `MOD_ROLE_IDS`
in Railway (comma-separated), no code change.

**Why:** Neither repo has a migration system; startup-ensure matches ava_bot's
own convention and finally puts dashboard-owned DDL in code (the older gap
flagged 2026-06-21 still stands for the three auth tables). Env-var gating
avoids hardcoding a role ID that only Railway knows.

**Rejected:** A manual one-shot migration script (fresh deploys would 500 until
run); a separate TRACKER_ROLE_IDS env (more knobs than the trial warrants —
revisit if mod-tier access ever needs to diverge from sample-preview access).

**Cross-repo note:** additive only — new tables, no changes to bot-owned
tables. Logged in data-system.md ownership table.

---

## 2026-06-21 — Established the shared-DB cross-repo contract

**Decided:** Documented that `ava_dashboard` and `ava_bot` share one Railway PostgreSQL database, with the contract written up at `E:\Obsidian\claude brain\Reference\data-system.md`. The bot owns/writes the data tables; this app mostly reads them and owns only `terms_acceptance`, `dashboard_sessions`, `user_preferences`.

**Why:** A schema change in the bot (rename/drop/retype a column) can break this app silently in production, because nothing in the bot's path tests the dashboard. The contract makes that coupling explicit.

**Found during setup:** Several tables this app depends on — `locations`, `plusones`, `manual_points`, `active_informants`, plus the three this app writes — have **no `CREATE TABLE` in either repo**. Their schema lives only in the live DB. `locations` is the highest-risk (maps + restock joins depend on it). TODO: run `pg_dump --schema-only` and backfill these definitions into whichever repo should own them.

**Rule going forward:** Additive changes (nullable column, new table) are safe. Destructive changes to a shared table require grepping `ava_dashboard/main.py` for the table/column first, and updating both repos in the same change.

---

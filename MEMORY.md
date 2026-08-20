# MEMORY.md
Decision log for ava_dashboard. Read this at the start of every session.

---

## 2026-08-04 — Catalog: grid view with card images, from PPT's own image URLs

**Decided:** `/catalog` gets a list/grid toggle. Grid tiles show the card
image, pulled from `catalog_cards.image_url`, which is populated from the
`imageCdnUrl*` fields PokemonPriceTracker already returns on every `/cards`
row — the same "already in a payload we're billed for" trick as `raw_price`.

**Those URLs point at `tcgplayer-cdn.tcgplayer.com`, and that was a real
decision, not an oversight.** I checked TCGplayer's Terms of Service and API
Terms first: the API Terms forbid "copy, reproduce, distribute, republish,
download, **display**… any part of the Site" without API access, and
separately forbid obtaining Site content "from a third-party." On a plain
reading that covers hotlinking their CDN. **The maintainer's call was to
proceed on the basis that the PPT API is a paid product and these URLs are a
documented field of it** — a defensible position, and materially different
from the alternative I had been considering (reverse-engineering TCGplayer's
URL pattern from `tcgplayer_id`). If TCGplayer ever objects or changes the
scheme, the fix is a re-stock, because we store PPT's field rather than
constructing the URL. **Do not "optimise" this into building URLs from
tcgplayer_id** — that would forfeit both that fallback and the rationale.

**`_ppt_image_url()` validates before storing.** https only, hostname must be
in `PPT_IMAGE_HOSTS`, credentials rejected outright. Parsed with `urlsplit`,
not string-sliced: a hand-rolled `split("@")` reads
`https://tcgplayer-cdn…@evil.com/x` backwards and accepts the wrong host.
This value is written to the DB and later rendered as an `<img src>` for
every visitor, so a surprise value in the vendor feed must not be able to put
an arbitrary origin into the page. Ten forms are covered by tests.
**`PPT_IMAGE_HOSTS` and the CSP `img-src` entry must change together.**

**`image_url` is COALESCEd on conflict, not overwritten.** A later response
that omits the image must not blank one already stored — only a real URL
replaces a real URL. Rows cached before this ship NULL and show a "No image
yet" placeholder until their set is re-stocked or refreshed; a missing image
is obvious, a wrong one isn't.

**Grid mode hides only the column-label row and `<tbody>`, keeping the table
in the DOM.** The filter controls live in the table's `<thead>` — moving them
would mean maintaining a second copy for this view, and hiding the whole
table would silently strip every filter on switching views. Verified filters
stay usable, and that filtering while in grid mode re-renders tiles.

**Fixed 5:7 aspect box + `loading="lazy"`.** Without the reserved box a
lazy-loaded grid reflows continuously while scrolling. Note for future
browser testing: **lazy images never load in the preview pane** because it
doesn't composite, so the intersection observer never fires — that is a
harness artifact, not a bug. Confirmed by flipping 8 tiles to `eager` and
watching all 8 load at 288x400.

**Not verified:** never run against live Postgres, and no card in the DB has
an image yet — existing rows need a re-stock/refresh to populate. Japanese
image coverage is unknown until JP sets are stocked.

---

## 2026-08-03 — Incident: the flagged constraint-rename risk actually hit

The live-DB risk called out in the 2026-08-02 "language column" entry below
materialized in production: `ensure_card_tracker_schema` started throwing
`DuplicateTableError` on every call after the first. Root cause was more
specific than "wrong constraint name" — the DO block's exception handler
caught `duplicate_object` (42710), but a duplicate **constraint name**
surfaces as `duplicate_table` (42P07), because a UNIQUE constraint's backing
index is itself a relation. Caught the wrong SQLSTATE class entirely.

**Real consequence, not just log noise:** `ensure_card_tracker_schema`'s DDL
loop has no per-statement try/except, so the exception aborted the whole
loop. Everything positioned after the constraint swap in
`CARD_TRACKER_SCHEMA` — `user_tracked_cards`, and `card_tracker_prefs`
(added in the *same* deploy that already had this bug live) — silently
never got created. And because this ran through `/api/card-tracker/refresh`
(the manual button + the 11pm scheduler) *before* `sync_watchlist`/
`run_ppt_ingest`/`run_ingest`/`run_scoring`, every one of those was skipped
too. `run_ingest` (One Piece pricing) has no other caller, so One Piece
prices were likely stale for as long as this was live. Pokemon pricing was
fine — the 3-hourly sweep calls `run_ppt_ingest` directly, bypassing
`/refresh` entirely.

**Fixed**: catch both `duplicate_object` and `duplicate_table`, and moved
the constraint swap to the END of `CARD_TRACKER_SCHEMA` — so this exact
failure mode can only ever block itself now, never the tables everything
else depends on. Self-healing on the next run, no manual migration needed
(`card_tracker_prefs` finally gets created the moment this deploys, since
it's now ordered before the risky statement).

**Lesson for next time a constraint gets renamed by guessed name**: don't
assume the exception class from first principles — a "duplicate X" error's
SQLSTATE depends on what kind of database object X's *backing structure*
actually is, not what you called it in the DDL. When in doubt, put the
risky statement last in a DDL list, not first.

## 2026-08-02 — Card tracker: tracked_cards gets a language column

**Decided:** `tracked_cards` now has `language TEXT NOT NULL DEFAULT 'english'`,
and its uniqueness widened from `(game, name, set_name, card_number)` to
include `language`. Driven by the portfolio search labeling English/Japanese
cards (catalog_cards already distinguishes them as separate PPT products) —
without this, adding a Japanese printing with the same name/set/number as an
already-tracked English one would have silently linked to the wrong card's
price history.

**Live-DB risk, flagged rather than hidden:** widening the constraint means
dropping the old one first, and the drop targets a **guessed** constraint
name (`tracked_cards_game_name_set_name_card_number_key`, Postgres's default
auto-naming for an unnamed inline `UNIQUE(...)` — never verified against the
actual production DB, since this session had no DB credentials to check
with). If the guess is wrong, the fix fails LOUD, not silent: every insert
into `tracked_cards` would start throwing "no unique constraint matches ON
CONFLICT" until someone finds and drops the real constraint by hand.
**If cards stop saving after this deploys, check `pg_constraint` for
`tracked_cards`'s actual unique constraint name first** — that's the likely
cause.

**Language now threaded through the whole pricing pipeline**, not just
search/add — `resolve_tcgplayer_ids`'s catalog match, `run_ppt_ingest`'s
nightly price fetch and search fallback, and both backdate paths all pass
`language` now. Skipping this would have meant a Japanese card saves fine
but silently never prices correctly (defaults to English at the PPT call).

## 2026-08-02 — Card tracker: opened to all paid members, one portfolio each

**Decided:** The card tracker moved from a single admin-only shared list to
per-member portfolios. Access is now gated by `get_current_user` (the same
dependency that splits the real dashboard from `/sample`) instead of
`require_admin` — any member with the paid premium role gets in, not just
`ADMIN_USER_IDS`. Each member can track up to `MAX_USER_PORTFOLIO_CARDS`
(100) cards of their own, added/removed via new
`/api/card-tracker/portfolio/*` routes.

**Data model — shared catalog, per-user membership, NOT per-user data.**
`tracked_cards`/`price_snapshots`/`card_scores` stay exactly as they were: one
row per card, shared by everyone. A new join table, `user_tracked_cards
(user_id, card_id)`, is the only per-member data. If two members track the
same Charizard, PPT is billed once a day, not twice — this was the deciding
factor over giving each member fully separate rows, given how tightly this
codebase already guards the shared PPT/JustTCG credit budget (see
`card_tracker.py`'s module docstring).

**Rejected: wiping existing data.** The initial ask was "clear out
everything," but on confirmation the actual want was to keep it — all
cards tracked before this change were linked to Discord user
`96718322170597376`'s new portfolio via a one-time migration insert (run by
hand against the live DB, deliberately NOT baked into the app's idempotent
startup schema — a specific Discord user ID has no business running on every
future deploy).

**Two queries now filter to `EXISTS (SELECT 1 FROM user_tracked_cards ...)`**
(`_MISSING_TODAY_SQL`, `_COVERAGE_SQL`, and the free-resolve query in
`resolve_tcgplayer_ids`, all in `card_tracker.py`) — a catalog row nobody
actually tracks (an old `watchlist.py` seed, or the last member who dropped
it) stops costing a credit every day. This is new: before per-user
portfolios existed, every row in `tracked_cards` was implicitly "wanted" by
whoever curated the admin list.

**`MAX_TRACKED_CARDS` raised 400 → 3000.** Sized against the PPT **API plan**
(20,000 credits/day) specifically — steady-state cost is ~1 credit/card/day,
so 3,000 cards is ~15% of the daily budget, leaving room for the catalog
backfill and grading calculator sharing the same PPT key. **If the PPT plan
ever changes, re-check this number** — it's not a generic default, it's
sized to this specific account's tier.

**Admin-only tools stay admin-only, not opened to members**: rematch,
reset-history, manual Backdate (30/60/180d), and bulk "Import a set" all
still require `ADMIN_USER_IDS`, now living behind a "Full Catalog" view
toggle admins can switch to (default view for everyone, including admins, is
"My Portfolio"). Deliberate: keeps every PPT/JustTCG-credit-spending trigger
in a small trusted group even as the member base — and therefore aggregate
portfolio size — grows. A member's own "Add a card" (free catalog search) is
the only way a regular member's action can spend credits, and only when the
card is genuinely new to the whole shared catalog — auto-backdated the same
way any first-time-resolved card already is (see the auto-backdate entry
directly below).

## 2026-08-02 — Card tracker: new cards auto-backdate 180 days on first resolve

**Decided:** The moment a tracked Pokemon card gets a `tcgplayer_id` for the
first time (free catalog match in `resolve_tcgplayer_ids`, or the paid-search
fallback in `run_ppt_ingest`), it now also gets an automatic 180-day PPT
`includeHistory` pull — the same call the manual admin "Backdate" button
makes, just automatic and unconditional. Reasoning: a brand-new card's graph
used to be a single flat point until someone remembered to click Backdate;
this makes every new card useful immediately.

**Cost is real and NOT gated by a confirm dialog**, unlike the manual
action: +2 PPT credits per newly-resolved card, no admin sign-off. Capped at
`AUTO_BACKDATE_RUN_CAP` (25) per ingest run, shared across both resolve
paths — importing/adding a whole set at once can't burn the day's credit
budget in one sweep. Cards beyond the cap still get today's live price
normally; they just don't get history until the next sweep's cap resets, or
a manual backdate.

**Shared helper**: `_backdate_one_card` (fetch + idempotent insert for one
card) backs both the manual `run_ppt_backdate` and the automatic
`auto_backdate_new_card` — same PPT call, same day-skip logic, so the two
paths can't drift apart.

## 2026-07-30 — Catalog: Japanese sets, as a multi-select alongside English

**Decided:** The catalog browses English and/or Japanese, via two independent
checkboxes (English ticked by default). Both can be on at once — they are
separate datasets shown together, not a display toggle, because PPT prices
the two printings as different products (`price_sources.CardRef.key()`
already includes language for exactly this reason).

**The DB needed nothing.** `catalog_cards` has had `language` in its unique
index and every query since it was built; it was simply never exposed. The
work was making `language` a LIST end-to-end rather than a single string.

**Three traps, all closed and tested:**

1. **An empty or unknown language must never become "no language filter"** —
   that would silently merge both printings into one list.
   `catalog._norm_languages()` always returns a non-empty allow-listed list,
   and the WHERE clause is always `language = ANY(...)`, so the single- and
   multi-language cases take one code path.
2. **Facets group by `(set_id, language)`, not `set_id` alone.** A set id
   present in both languages would otherwise collapse into one row with a
   summed count — two genuinely different products merged. Verified against a
   fixture where `SV: 151` exists in both.
3. **The frontend still keys the set filter on `set_id` alone**, so the two
   facet rows are merged back into ONE picker option (counts summed, EN/JP
   badge only when a set is single-language). Left split, they'd render as
   duplicate-looking entries that both toggle the same filter. Selecting that
   option correctly returns both printings — which is what "that set" means
   when both languages are ticked.

**Language is a SCOPE, not a facet.** The "a facet never applies its own
filter" rule still holds for set/rarity, but language applies to *both* facet
queries — it selects the dataset rather than filtering within it.

**Unticking the last language is refused** rather than showing an empty
catalog. The server would fall back to English anyway, so an empty selection
would make the UI lie about what it's showing.

**Stocking is per-language and deliberately separate from browsing.** The
admin panel has its own language dropdown; "stock the Japanese sets I'm
missing" is a different question from "show me Japanese cards". Stocked-ness
is per `(set_id, language)` — a set stocked in English says nothing about its
Japanese printing. The bulk backfill buttons and the in-process
`_catalog_known_empty` / `_catalog_refreshed_sets` trackers are all
language-keyed for the same reason: one language's misses must not suppress
the other's retries.

**The startup seed stays English-only** (`CATALOG_SEED_LANGUAGES`, default
`english`). It runs unattended on every deploy and seeding a language costs
real credits, so Japanese is stocked deliberately from the admin panel; set
the env var to `english,japanese` to opt in.

**The nightly price sweep DOES cover both**, because a stocked Japanese set
whose prices never refresh would go stale forever. They share the one daily
credit budget — the existing reserve/stop guard bounds total spend, so this
spreads the budget rather than doubling it.

### Found while testing — pre-existing, NOT from this change

Three suites were already failing on `main` before this work (confirmed by
stashing). Fixtures repaired; one is a real open question:

- **`trainer_kits` is now an unreachable era group.** Commit `e008919`
  ("fix era grouping for dash- and space-separated set names") added bare-space
  release-code matching for names like `XY Base Set` — which also catches
  *every* real Trainer Kit name, since they all begin with an era code.
  `XY Trainer Kit: Sylveon & Noivern` → `xy`, `EX Trainer Kit Latias` → `ex`,
  and so on for BW/HGSS/SM/DP. Only the bare string `Trainer Kit` still
  reaches the group. Recorded current behaviour in the test rather than
  silently "fixing" either side — **decide whether Trainer Kits should group
  by era (then drop the dead group from `ERA_ORDER`) or stay standalone
  (then the keyword rule needs to run before the prefix match).**
- `fetch_ppt_card_prices` gained a third return value (`daily_remaining`);
  a test stub still returned two.
- Templates now read `request.session` directly (the inventory nav gate), so
  a fixture passing `request={}` no longer renders.

---

## 2026-07-30 — Inventory: sports-card pick tool, admin-only, unrelated to restocks

**Decided:** `/inventory` — a standalone sports-card inventory / eBay pick
tool for one seller (a friend of the maintainer's), built as a page on this
dashboard for deploy convenience. `inventory.py` + `inv_*` tables. **Unrelated
to the restock-tracking data this app otherwise reads** — no join, no shared
meaning, just sharing this app's DB and Discord auth because it's convenient.
Admin-only (`require_admin`), like the card tracker — this is single-seller
tooling, not a member-facing feature, so it does NOT follow the newer
guest-open pattern `/catalog` and `/grading-calculator` use.

**Core design (from the requester's own spec, not derived here): SKU-as-
address.** eBay's Custom Label (SKU) field rides along on the sold order and
packing slip for free. Make the SKU the storage bin code, and the pick
location arrives with the sale — no lookup, no sync, no DB hit at pick time.
Bin-level addressing, not slot-level: a bin holds ~25 cards with no internal
order, so pulling one doesn't require renumbering the rest. A bin's SKU is
therefore **shared by every card in it** — deliberate, not a bug; eBay's
Custom Label has no uniqueness requirement.

**Bin occupancy is COMPUTED, never a stored counter.**
`COUNT(cards WHERE bin_id = X AND status IN ('available','listed','sold'))`.
That's what makes "free the bin slot on `picked`, not on `sold`" free: a
status change is what the COUNT sees, with nothing to keep in sync and no way
for a counter to drift from the cards table. Verified directly: 2 cards
marked `sold` still occupy their bin (occupied=2); marking one `picked` drops
it to 1 with no other write.

**Status transitions are enforced in code, not a DB CHECK constraint** —
`inventory.validate_transition()` / `_TRANSITIONS` — specifically so a
rejected move gets an actionable message ("can't move available -> picked;
allowed: listed, sold, at_grader, on_consignment") instead of a generic
constraint failure. `shipped` is terminal; `sold`→`picked`→`shipped` cannot be
skipped; `returned` re-enters only via `available` ("restow as new" — no
attempt to reconstruct the old address).

**Fill pointer is sequential-fill per zone, auto-creating a bin when the zone
is full.** `assign_bin()`: lowest-seq active bin under capacity wins; if none,
create the next bin in sequence at the default capacity. Verified: fills
bin 1 to capacity before touching bin 2; once every bin in a zone is full,
creates a new one; two zones (`STD`, `PREM`) fill completely independently.
Zones seeded once at startup (`DEFAULT_ZONES`) and never overwritten on
redeploy, so a seller who renames a zone later isn't silently reverted.

**NOT built, and nothing here fakes it:** eBay OAuth + Sell Inventory API
sync, photo intake through a vision model, checklist-database resolution
(year+set+number → canonical fields), Fulfillment API polling for the pick
queue. Intake is manual entry. `needs_location` and `cant_find` are real
flags with working queries and UI, but nothing populates `needs_location`
yet — that's the seam an eBay poller would write into, per the design notes
("listings created outside the tool ... drop anything without a valid
location SKU into a Needs Location inbox").

**Every status change is logged to `inv_events`, append-only.** An audit
trail for "where did this go" is cheap to write now and expensive to
reconstruct later if skipped.

**Not yet verified:** never run against live Postgres. The transition table
and the fill-pointer are unit-tested against a fake pool (12 legal + 6 illegal
transition cases; 6 fill-pointer scenarios including the sold/picked
occupancy behavior); the routes and real asyncpg queries are not.

**2026-07-30, same day:** opened `/inventory` to a second Discord role
(`INVENTORY_ROLE_IDS`, default `1528530585960710338`) without widening
`require_admin`. New `require_inventory_access()` gate, separate from
`require_admin`, checking a session flag (`inventory_access`) set at login
alongside the existing `mod` / `all_mods` flags — same pattern, own role set.
**Deliberately not reusing or extending `require_admin`**: that function
gates the card tracker too, and this role must open only the pick tool.
Verified the two gates diverge correctly for a non-admin holder of the role
(opens `require_inventory_access`, `require_admin` still False) before
wiring all 7 inventory API routes plus the page route to the new gate.

**Nav link added to the 11 templates carrying the real Nexus Playground
dropdown**, gated on `request.session.get('inventory_access', False)` read
directly in Jinja rather than a new per-route context variable — `request`
is already passed to every `TemplateResponse`, and the flag already covers
admins (`role OR is_admin_user`, set once at login), so one condition serves
both cases. Deliberately reading the session in the template instead of
touching 16 route handlers' context dicts individually, given how much of
`main.py` has changed outside this session — a template-only diff was the
safer edit. **Skipped `sample*.html`, `guest_home.html`, `landing.html`** on
purpose: those are demo/anonymous surfaces that never go through the real
Discord role flow, and they already omit the Tracker link for the same
reason — Inventory follows that existing precedent rather than setting a new
one. Verified against real `request.session`-shaped objects in three states
(flag True, flag False, key absent entirely) that the link shows only when
access is actually granted and never raises on a missing key.

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

**Guarantee: one price per Pokémon card per UTC day.** `run_ppt_ingest` is
**gap-filling, not a scheduled batch** — it prices only cards with no snapshot
for the current UTC day, and a sweep runs every 3h
(`TRACKER_SWEEP_INTERVAL_S`) plus shortly after boot.

**Idempotence is what makes this affordable, and it is the property to
preserve.** A card already priced today is skipped, so eight passes a day cost
the same credits as one — which is why the sweep can recover from a restart,
a mid-run 429, or a transient per-card error within hours instead of never.
Anything that makes a run re-price already-covered cards silently multiplies
the daily spend by the sweep count.

**Ordering is neediest-first** (`last_priced ASC NULLS FIRST`). The first
version used `ORDER BY id LIMIT n`, which re-priced the same first N every
night and would never have reached cards beyond the cap at all — a silent
permanent gap once the list exceeded 150. UTC is the day boundary, matching
the JustTCG history backfill's existing dedupe, so the two can't disagree
about what "today" is.

**Resolution never leaves a card stranded:** catalog first (free), then a
one-time PPT search (`resolve_ppt_tcgplayer_id`, ~5 credits, capped at
`TRACKER_PPT_RESOLVE_CAP` per run) for cards whose set isn't stocked. Reuses
`_ppt_pick_card` rather than a second matcher.

**What can still break the guarantee, and it's arithmetic:** if tracked cards
exceed PPT's daily credit allowance, no scheduling fixes it. Coverage is
therefore reported as a number (`priced_today` / `tracked_total` /
`missing_today`) in the refresh summary and on the tracker page — a shortfall
must be visible, not a silent hole in the history.

**Pokémon has NO history backfill.** JustTCG returns price history so a missed
One Piece day self-heals; `fetch_ppt_card_prices` returns current price only.
A Pokémon day missed entirely is permanently lost — which is precisely why the
sweep exists rather than a single nightly run.

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

**eBay links carry Partner Network tracking, from config not code.**
`EBAY_AFFILIATE_PARAMS` holds the EPN query string verbatim
(`mkcid=…&mkrid=…&campid=…&toolid=…&mkevt=1`). Deliberately opaque: EPN has
changed its link format over time and the rotation id is site-specific, so
hardcoding a parameter set would be a guess that silently stops earning once
stale. Nothing to redeploy when EPN changes it, and no campaign id in git.

**It fails closed to a plain link.** Unset, malformed, or yielding no usable
pairs → the ordinary eBay search, logged as a warning. A broken campaign
string costs revenue; it must never cost a member a working link. `_nkw` is
refused from the affiliate string because it carries the search terms — one
slipping through would send every card to the same page.

`rel="sponsored"` and the on-page disclosure appear **only** when tracking is
actually configured (`catalog.ebay_affiliate_enabled()` → template flag).
Declaring a link sponsored when it isn't would be false, and disclosure is an
EPN/FTC obligation, so both track the real setting rather than being
hardcoded.

**TCGplayer links are NOT affiliate.** Only the eBay fallback is monetised —
and note that as sets get re-stocked and `tcgplayer_verified` fills in, the
eBay share of clicks shrinks. If monetisation matters, TCGplayer's own
program is where the volume ends up.

**Sets in the filter are grouped by era** (`card_eras.py`), newest series
first, from TCGplayer's "Every Pokémon TCG Set in Order" (read 2026-07-28) —
all 128 English expansions plus the promo/misc groups.

**New sets mostly classify themselves, which is the point.** Three passes, in
order: an explicit name in `SET_ERAS`; then an **era prefix** on the name —
PokemonPriceTracker labels sets `"SWSH: Crown Zenith"`, `"ME: Ascended
Heroes"`, so next year's release lands in the right era with no edit here;
then keyword rules for promos / McDonald's / POP / Trainer Kits. Anything
still unmatched goes to **"Other" rather than being guessed** — a set filed
under the wrong era is worse than one in a visible catch-all.

Names match loosely (case, spacing and punctuation stripped), so
`Scarlet & Violet—151`, `Scarlet & Violet 151` and `SV: 151` all resolve to
the same era. Subsets (Trainer Gallery, Shiny Vault, Galarian Gallery,
Classic Collection) are listed with their parent's era so they don't scatter.

**Promos are a standalone group, and that rule runs FIRST.** "SWSH Black Star
Promos" belongs with the other promos, not inside Sword & Shield — it isn't a
main-series expansion. The promo check happens before the name lookup and the
prefix rule precisely so a series name in the title (or an `SV:` prefix)
can't drag it back into that series. Energies are *not* promos and stay with
their era.

**An era header is itself a checkbox: ticking it selects every set in that
era.** It operates on the sets *currently visible*, so with a search active it
takes only the matches — selecting sets the user can't see would produce a
filter they can't explain. The header shows indeterminate when only some of
its sets are picked, and `toggleEra` updates the child boxes in place rather
than re-rendering, because a re-render resets the list's scroll position out
from under the cursor.

**Set and rarity narrow each other (faceted filtering), and one rule makes it
work: a facet's own counts NEVER apply its own selection.** Rarity option
counts are computed with the set/price/search filters applied but NOT the
rarity filter; set counts the mirror. Apply a facet to itself and picking one
rarity would collapse the rarity list to just that rarity — no way to add a
second. `catalog._facet_counts()` runs two queries per page request rather
than reusing `_build_filters` uniformly, specifically to hold that asymmetry.

Counts ride along on the existing `/api/catalog/cards` response
(`with_facets=True`) instead of a second round-trip, so the options can never
disagree with the rows actually shown — no separate "did the facets refresh
yet" state to get out of sync.

**A selected option that now matches zero cards stays listed, at 0.** Dropped
instead, and a rarity that only existed in a set you just deselected becomes
permanently stuck on — the filter, still active, with no control left to turn
it off. `applyFacets()` re-adds any selected value missing from the response
before rendering.

**Picker option ownership moved to `applyFacets()`, and `loadFacets()` no
longer sets `pickerData[key].options`.** Two writers would race — the global
list (from `/facets`) landing after the cross-filtered one (from `/cards`)
would silently undo the narrowing on every keystroke. `loadFacets()` still
owns coverage stats, era order and the admin stock picker.

**The picker panel is larger on desktop** (`min-width: 900px`: 330px wide,
480px list vs. 250×240 on mobile) and `positionPicker()` now shrinks the list
to fit a short window rather than letting the panel run off-screen — measured
against the CSS max-height fresh on every open, so a shrink from a previous
short window doesn't linger after the browser is resized taller.

**`ERA_ORDER` lives only in `card_eras.py`.** The API ships `era_order` and
`era_labels` to the client rather than the template mirroring the list — one
place to edit when a new series starts.

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

## 2026-08-10 — Paid subscriptions via a non-Discord account entry point

**Decided:** Added a standalone email-based account system (`billing.py` +
`/signin`, `/account`, `/api/billing/*`) with Stripe subscriptions and a
7-day card-required trial. Entitlement unlocks the two Nexus Playground
tools (Grading Calculator, Card Catalog) at full tier — nothing else.

**The load-bearing invariant:** a paid account NEVER gets `session["user"]`.
It gets `session["account_id"]`. Every Discord-gated route reads
`session["user"]`, so all of them stay closed *by construction* rather than
by remembering to add a check. A subscriber's user dict is
`{"id": None, "guest": False, "account": True}` — no Discord id to be
mistaken for a member, and `guest: False` because full access is the thing
they bought. Access is opt-in per page via `_viewer_context(allow_account=)`,
which only the two Playground pages pass.

**Why Stripe hosted Checkout:** card details never reach this server, which
keeps us out of PCI scope. Do not replace it with an embedded card form.

**Why the webhook is CSRF-exempt:** it's server-to-server, so no cookie
exists to double-submit. Its HMAC signature *is* its authentication, and it
is mandatory — an unset `STRIPE_WEBHOOK_SECRET` returns 503 rather than
degrading to "trust anything". Stripe is the source of truth;
`acct_subscriptions` is a mirror written only by the verified webhook.

**Entitlement requires status AND an unexpired `current_period_end`**, so a
missed cancellation webhook can't grant access forever. `past_due` (a failed
payment) does not entitle.

**Event ordering:** `customer.subscription.created` can arrive before
`checkout.session.completed`, when no customer link exists yet and the
subscription would be dropped as "unknown customer". `checkout.session.completed`
therefore re-fetches the subscription from Stripe after linking.

**Rejected:** Discord-linked paid roles (defeats the purpose — the ask was a
non-Discord entry point); passwords (a credential to store, leak and reset,
for no gain over a single-use emailed link); trial without a card (converts
far worse and invites throwaway-email abuse); granting subscribers
`session["user"]` with a synthetic id (one missed check anywhere would sell
the entire restock dashboard for the price of a Playground membership).

**New env vars — the feature is inert until these are set:**
`STRIPE_SECRET_KEY`, `STRIPE_PRICE_ID`, `STRIPE_WEBHOOK_SECRET`,
`STRIPE_TRIAL_DAYS` (default 7), `RESEND_API_KEY`, `LOGIN_EMAIL_FROM`.

**Cross-repo note:** additive only — three new `acct_*` tables, no bot-owned
table touched.

---

## 2026-08-10 — Card Tracker included in the paid subscription

**Decided:** A subscription now also opens `/card-tracker` and its seven
own-portfolio endpoints (`portfolio`, `portfolio/search`, `portfolio/add`,
`portfolio/remove`, `prefs` GET+POST, `history`) via a new
`get_member_or_subscriber` dependency. Every `require_admin` tracker tool
(`list`, `refresh`, `rematch`, `reset-history`, `remove`, `backdate`,
`import/*`) is unchanged. The 250-card cap applies to subscribers too.

**Why:** the Stripe product description sells the tracker; shipping it
behind a Discord gate would misdescribe what people are paying for.

**How a subscriber owns a portfolio:** `user_tracked_cards.user_id` is a
BIGINT holding Discord ids. A subscriber's row is keyed by their
`acct_accounts.id` in the same column. This is safe because that table's
`CHECK (id < 1000000000000000)` sits ~11x below the smallest real Discord
snowflake (~1.1e16), so the ranges cannot overlap and every existing query
works untouched. That constraint is now load-bearing for portfolio
ownership, not just tidiness — do not raise it.

**Revises the earlier entry:** the subscriber user dict was
`{"id": None, ...}`; it is now `{"id": <account id>, ...}`. The
session["user"] invariant is unchanged and still the real gate — the
dashboard, map, analytics and inventory stay closed by construction.
`is_admin`/`is_mod` on the tracker page are now explicitly gated on a
Discord session existing first, so a local id is never compared against
ADMIN_USER_IDS.

**Rejected:** read-only tracker access for subscribers (much weaker than
what's being sold); a synthetic Discord-shaped id (invents collision risk
the CHECK constraint already rules out); a separate subscriber portfolio
table (duplicates every tracker query for no benefit).

**Cross-repo note:** none — `user_tracked_cards` is owned by this repo
(`card_tracker.py`), not the bot.

---

## 2026-08-10 — Buy-now option, and one trial per account

**Decided:** Checkout offers two paths — start the free trial, or subscribe
immediately and be billed today. A returning customer who has already used
a trial is only offered the paid path.

**Why the eligibility check exists:** Stripe grants a trial on EVERY
Checkout Session that passes `trial_period_days`. It has no built-in
one-trial-per-customer rule, so without this someone could cancel and
re-subscribe indefinitely and never pay. `billing.account_has_used_trial()`
answers it from our own mirror (`acct_subscriptions.trial_end IS NOT NULL`),
which is the reason `trial_end` is mirrored at all.

**The security shape:** the browser flag is `skip_trial` — a DECLINE, never
a request. The server computes
`bool(STRIPE_TRIAL_DAYS) and not declined and not already_used`. A hostile
body can only cost the sender their own trial, never grant one.

**Fails closed:** a DB error or missing account in the eligibility check
returns "used", so a hiccup costs one person a free trial rather than
opening unlimited ones.

**When not granting a trial, `trial_period_days` is omitted entirely** —
not set to 0, which Stripe rejects.

**Public pages still advertise the trial unconditionally.** Landing and
guest-home visitors are anonymous, so eligibility is unknowable there;
`/account` is where the offer is corrected once we know who they are.

**Rejected:** trusting a browser-supplied `trial` flag (a repeat-trial
hole); checking eligibility against Stripe's API per checkout (a network
round-trip for something our own mirror already answers).

---

## 2026-08-19 — Custom portfolios with cost basis, plus sealed product

**Decided:** New `portfolios.py` module owning `portfolios`,
`portfolio_lots` and `sealed_products`. Up to 5 portfolios per person and
500 distinct items across all of them. The Card Tracker watchlist is
unchanged and still works exactly as before.

**Why lots, not one row per card:** `user_tracked_cards` is
`PRIMARY KEY (user_id, card_id)` — a watchlist with no quantity and no
price. Cost basis needs per-purchase rows, because buying the same card
twice at different prices on different dates is two facts, and averaging
them destroys the information a return is computed from.

**Data captured per lot:** quantity, condition/grade, price paid each,
fees+shipping, purchase date, bought-from, plus sale date / sale price /
selling fees for closed positions, and notes. Fees are included in cost
basis deliberately — a return that ignores shipping flatters every eBay
purchase. Grade matters because raw vs PSA 10 is a different asset.

**Money is Decimal end to end**, NUMERIC in the DB, serialized with str()
not float(). Floats would put rounding error into someone's ROI.

**Unpriced is not zero.** Sealed product has no price source today, so
those lots report as unpriced and are excluded from market value; the
unrealized percent divides by the basis of PRICED lots only. Counting an
unpriced item's cost with no value would show a loss that isn't real.

**THE COST PROPERTY (explicitly requested):** a lot references the SHARED
`tracked_cards` catalog, never a private copy, and card_tracker's
`_MISSING_TODAY_SQL` / `_COVERAGE_SQL` now match "on any watchlist OR in
any portfolio". A card in the tracker and in five members' portfolios is
priced ONCE per day. The 500-item cap counts distinct items, not lots, for
the same reason. `ensure_card_tracker_schema` now also creates the
portfolio tables, because the cron scripts call only that function and the
ingest query would otherwise hit a missing relation.

**Sealed product — UNRESOLVED:** PokemonPriceTracker's documented v2
endpoints are `/cards` and `/sets` only. Whether it serves sealed product
is **unconfirmed** — the API key lives only on Railway, so it could not be
probed. Rather than write speculative client code, sealed SKUs are seeded
per set from the existing set list (Booster Box / ETB / Booster Bundle),
admin-triggered and idempotent. Prices stay blank, per the decision to
build it and leave prices blank. If a sealed endpoint exists, the upgrade
is filling `ppt_product_id` and adding a sealed branch to
`current_prices()`.

**Rejected:** extending `tracked_cards` with a `kind` column (its UNIQUE
key includes card_number, so sealed rows collide on empty string, and the
scoring formulas assume single cards); a generic `assets` table (rewrites
the shared catalog every existing tracker query depends on); 250 items per
portfolio (would multiply nightly ingest cost fivefold).

**Cross-repo note:** additive only — three new tables, no bot-owned table
touched. `card_tracker.py`'s two cost-control queries were widened.

---

## 2026-08-19 — Tracker Management page (admins + server mods)

**Decided:** New `/tracker-admin` page under Admin Tools, gated by a new
`require_server_mod` (ADMIN_USER_IDS **or** `session["mod"]` from
MOD_ROLE_IDS). It holds three jobs: add/re-seed a single card, import a
whole set, and seed sealed product per set.

**Why server mods and not `all_mods`:** the all_mods group exists to open
the invite network to role managers. Every action on this page either
spends PokemonPriceTracker credits or writes to a catalogue every member
reads, so it follows the narrower MOD_ROLE_IDS set. A role manager reaches
/invite-network but not this page.

**What moved:** the set-import panel and its ~119 lines of JS came off
`/card-tracker`. `_require_tracker_admin` widened from admin-only to
admins + server mods to match the page it now lives on. Sealed seeding
moved off curl-only (`require_admin`) onto this page's gate.

**What did NOT move:** every destructive global tool — reset-history,
refresh, rematch, remove, the full shared catalog list — stays
`require_admin`. Seeding and importing are additive; wiping history is not.

**Pokemon only**, confirmed with the user: PokemonPriceTracker is the only
price API, so the single-card search sends `game: "pokemon"` and the sealed
seeder reads only `game='pokemon'` sets. The set importer still offers One
Piece because it uses the free catalog APIs, not PPT.

**Nav:** the link sits inside the Admin Tools menu on all 14 templates
carrying that menu, shown to whoever can open the menu. The route is the
real gate — a link that 403s is as bad as a page nobody can find.

**Rejected:** putting these controls on /admin (that page is
ADMIN_USER_IDS-only and mods need these); leaving set import on the card
tracker page (the ask was to consolidate).

---

## 2026-08-19 — Positions, FIFO partial sales, and estimated cost

**Decided:** The holdings table now groups by ITEM (a "position"): one row
per item with total held, weighted-average cost, value and gain,
expandable to show every individual purchase with its price, date and
source. Lots remain the source of truth; a position is a view over them,
never a stored total that could drift.

**Selling is FIFO with automatic lot splitting.** You sell N units of a
position; the oldest purchase is consumed first, and when the sale is
smaller than that purchase the lot SPLITS — the sold units become their
own closed lot carrying the original price, date and source, and the rest
stays open.

**Why not weighted average (the user's first instinct):** it works and is
simpler, but it blends purchases permanently, so once units are sold you
can no longer tell how a specific buy performed. Worked example given to
the user — 1 @ $119.99 (March) + 2 @ $142.50 +$11.20 fees (July), sell one
at $175: FIFO realizes $55.01, weighted average $36.27, specific-July
$26.90. All defensible; FIFO keeps per-purchase records intact and
produces the same grouped UI.

**Fee arithmetic is exact, not approximate.** Purchase fees follow the
units they were paid on ($11.20 over 2 units -> $5.60/$5.60), and selling
fees are split across consumed lots with the LAST share absorbing the
rounding remainder. Verified that parts always sum back to the original
total — a cent leaking per partial sale is how a portfolio stops
reconciling.

**Sale runs in one transaction with `FOR UPDATE`** on the item's lots: a
split writes two rows, and two concurrent sales could otherwise oversell.

**A purchase with no date sorts LAST in the FIFO queue**, not first — an
unknown date must not jump ahead of a purchase we can actually date.

**Blank price paid defaults to market price** (from the shared
price_snapshots history, so no API call), and the row is flagged
`cost_is_estimated` and labelled in the UI. BLANK and ZERO are kept
distinct: blank means "I don't remember", zero means "it was free" (pack
pull, gift). Collapsing them would either erase free pulls or invent a
cost for them. Editing in a real number clears the flag; leaving it blank
on an edit keeps what's there rather than re-estimating over a correction.
Sealed has no price source, so its cost stays 0 and is NOT labelled an
estimate — there is nothing to estimate from.

**Rejected:** weighted-average cost basis (above); specific-lot selection
(more accurate still, but an extra choice per sale — it is a superset of
FIFO and can be added later without rework); a separate sales table
(splitting lots keeps one source of truth).

**Not advice:** which cost-basis method suits someone's tax records varies
by jurisdiction; the user was told to ask someone qualified rather than
given a recommendation.

---

## 2026-08-19 — Sealed product comes from the price API, per set

**Decided:** Removed the generator that created the same three SKUs
(Booster Box / ETB / Bundle) for every set. It invented products that were
never printed and missed ones that were. Sealed product is now imported
from PokemonPriceTracker's own per-set listing.

**The endpoint is still UNVERIFIED**, so the design fails loudly rather
than guessing: the path is env-configurable
(`POKEMONPRICETRACKER_SEALED_PATH`, default `/sealed,/sealed-products,
/products`), candidates are tried in order with a 404 moving to the next,
and the parser reads several candidate key names per field. Nothing is
written without a human seeing it — Look up returns the parsed products
AND a raw sample, and the Save button stays disabled until a successful
look-up.

**Import re-fetches server-side** rather than trusting a product list
posted by the browser: this writes to a catalogue every member picks from.

**Sealed can now carry a price.** `sealed_products.market_price` +
`price_updated_at`, filled from the API record when it has one.
`current_prices()` and `latest_unit_price()` both read it, so sealed
holdings can be valued and a blank "price paid" can default from it. A
product the API never priced stays None and reports as unpriced, never
zero.

**Deletion (`/api/portfolios/sealed/purge`) is ADMIN-only**, stricter than
the rest of the sealed tools, because `portfolio_lots.sealed_id` is
ON DELETE CASCADE — deleting a product someone holds deletes their
holding. Default mode is "unused", which spares anything a member holds.
"all" requires a dry run showing the damage, a confirm dialog, and the
typed phrase DELETE ALL SEALED, and logs at warning level.

**Rejected:** writing speculative client code against an assumed response
shape (hence preview-first); keeping the generic generator as a fallback
(a button that creates known-wrong data is a foot-gun, not a safety net).

---

## 2026-08-19 — pokemontcg.io flakiness, and a broken import panel

**Diagnosis:** the "Couldn't fetch the set list: 500" error was NOT our
bug. Measured from this machine: **8 of 10 identical requests to
api.pokemontcg.io failed** (instant 500s and Cloudflare 502s), while the 2
that succeeded took 7-17 seconds. It is a free service and goes down for
stretches. Isolating individual query params was a red herring — the same
request both succeeded and failed minutes apart.

**Fixes for the outage:** `set_import._pokemon_get()` retries 4 times with
linear backoff, retrying only 5xx/transport errors (a 4xx means the request
itself is wrong). `api_import_sets` caches a successful set list for 6h and
serves the EXPIRED cache during an outage, flagged `stale` with its age, so
the panel keeps working. Only a cold cache produces an error, and that
message now explains it's an upstream outage instead of printing a raw
exception and URL at the user.

**A bug of mine, found while investigating:** when the set-import panel was
moved to /tracker-admin its JS was REWRITTEN rather than moved, and it did
not match the API at all — it sent `set_name` where the route reads
`set_id`, sent a list of card rows where the route takes
`exclude_numbers` (card numbers to leave OUT), read `c.already` where the
response has `already_tracked`, and never populated the rarity filter from
`data.rarities`. The panel could not have worked.

**Why nothing caught it:** no test compared the page's payload to the keys
the route reads. `test_importpanel.py` now extracts the body keys each
route reads via AST and asserts the page sends them.

**Rule going forward:** when moving UI between pages, move the JS verbatim
first and refactor after — a rewrite silently drops the contract.

---

## 2026-08-20 — Sealed import: real endpoint, and the SHARED API budget

**Confirmed from production logs:** the sealed endpoint is
**`/sealed-products`**. `/sealed` 404s every time. Candidate order was
wrong, so every set cost two calls — a bulk run of 60 sets spent 60 calls
discovering the same 404. `/sealed-products` is now first, and a working
path is cached for the process so it is never re-probed.

**THE BIG ONE — one key, one budget.** `POKEMONPRICETRACKER_API_KEY` is
shared with the catalog backfill, the grading calculator and the nightly
tracker ingest. A bulk sealed import drove `daily-remaining` to **0**,
which starves all of them. Guards added:
  * `PPT_DAILY_RESERVE` (50) — bulk import stops while that many calls
    remain, leaving them for the features members use.
  * `daily_exhausted` is now a distinct status from `rate_limited`: a
    minute window clears in seconds, a daily allowance does not.
  * `Retry-After` is read and surfaced.
  * `PPT_SEALED_LIMIT` dropped to 25 — their own tip says a bigger limit
    costs more calls per request.

**Most sets have no sealed product** (old sets, promos, Japanese-only), and
each empty answer costs a call. New `sealed_set_checks` table records what
was asked and what was found; a bulk run skips sets found EMPTY within
`SEALED_EMPTY_CHECK_TTL_DAYS` (30). Sets that DID have product are always
re-checked, since that is how prices refresh. The skip list fails OPEN — an
unreadable log means check everything, never skip everything.

**Rule:** any new feature that calls PokemonPriceTracker shares this budget.
Bulk operations must respect the reserve and be resumable, not
fire-and-forget.

---

## 2026-08-20 — Sealed candidate sets come from the price API, newest first

**Why the bulk import found almost nothing:** the candidate list was
`SELECT DISTINCT set_name FROM catalog_cards ... ORDER BY set_name` —
alphabetical. A run therefore started on ADV Expansion Pack, ADV-P
Promotional cards, Alternate Art Promos, Aqua Deck Kit… vintage, promo and
Japanese-only sets that have no sealed product at all. The daily allowance
was spent before reaching the modern sets (Prismatic Evolutions and
friends) where product actually exists. 60 sets checked, 1 product found.

**Fix (the user's suggestion, and it was right):** use the same source the
card catalog uses — `price_sources.fetch_ppt_sets()`. One call, cached a
day, and it gives PPT's OWN set names, so there is no name-matching
guesswork between what we ask and what it knows. Critically it also carries
**release dates**, and `fetch_ppt_sets` already sorts newest first.

**Bulk runs now:** newest first, with a **year floor** (default 2020) so
vintage sets are excluded outright, and sets previously checked and found
empty are skipped. Falls back to stocked catalog sets if /sets is
unreachable, rather than showing an empty dropdown.

**Lesson:** when two features talk to the same API, they should share the
same identifiers from the same source. The catalog had the right set list
all along; the sealed import invented its own from a derived table.

---

## 2026-08-20 — Prepaid credits: opt-in overage on the sealed import

**Decided:** `daily-remaining=0` is no longer a hard stop. The account has
prepaid credits, so a bulk sealed import can continue past the included
daily allowance — but ONLY after an explicit confirmation that states what
will run and what it will cost.

**The shape:**
  * `fetch_ppt_sealed(..., allow_over_quota=False)` — default is still to
    stop. `daily_exhausted` is returned unless the caller opted in.
  * Opting in also lifts `PPT_DAILY_RESERVE`, because that reserve exists to
    protect the tracker and catalog from a bulk run, and paying for the
    overage is precisely what makes that unnecessary.
  * The per-minute window is NEVER bypassed — nothing can buy past it. The
    opt-in just means waiting out each `Retry-After` and resuming.
  * The confirm names: sets checked so far, products added, sets still to
    check, and "up to N credit(s)".
  * Paid runs log at WARNING with `PAID_OVERAGE=True`.

**Resumption is client-side and bounded**: a run continues in rounds,
waiting out each minute limit with a visible countdown, capped at 40 rounds
and stopping if a round makes no progress. Long-held server requests were
rejected — they time out and give no feedback while money is being spent.

**Rule:** any future bulk operation against a paid API follows this shape —
default stop, explicit opt-in, cost stated before it runs, and a bounded
resumable loop rather than one long request.

---
